import atexit
import logging
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import httpx
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

logger = logging.getLogger(__name__)

_COMMENT_RE = re.compile(r'^\s*/\*.*?\*/\s*', re.DOTALL)

_CREATE_TABLE_RE = re.compile(
    r'^\s*CREATE\s+TABLE\s+"([^"]+)"\."([^"]+)"\."([^"]+)"\s+AS\s+',
    re.IGNORECASE,
)

_CREATE_VIEW_RE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"([^"]+)"\."([^"]+)"\."([^"]+)"\s+AS\s+(.+)',
    re.IGNORECASE | re.DOTALL,
)

_DROP_RE = re.compile(
    r'^\s*DROP\s+(TABLE|VIEW)\s+IF\s+EXISTS\s+"([^"]+)"\."([^"]+)"\."([^"]+)"',
    re.IGNORECASE,
)


@dataclass
class KolkhisCredentials(Credentials):
    backend_url: str = "http://localhost:8000"
    auth_token: str = ""
    worker_url: str = ""  # Deprecated: kept for backward compatibility with existing profiles

    @property
    def type(self) -> str:
        return "kolkhis"

    @property
    def unique_field(self) -> str:
        return self.backend_url

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("backend_url", "database", "schema")


class KolkhisCursor:
    """DB-API 2.0 style cursor that routes SQL through the backend."""

    def __init__(self, backend_url: str, session_id: str, auth_token: str):
        self._backend_url = backend_url
        self._session_id = session_id
        self._auth_token = auth_token
        self.description: Optional[list] = None
        self._rows: list = []
        self.rowcount: int = -1

    def execute(self, sql: str, bindings: Any = None):
        if bindings:
            raise DbtRuntimeError("Parameterized queries not supported by Kolkhis adapter")

        headers = {"Authorization": f"Bearer {self._auth_token}"}
        with httpx.Client(timeout=300) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/session/{self._session_id}/query",
                json={"sql": sql, "fetch_results": True},
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") == "failed":
            raise DbtRuntimeError(data.get("error", "Query failed"))

        columns = data.get("columns") or []
        rows = data.get("rows") or []

        self.description = [(col["name"], col.get("type", "VARCHAR")) for col in columns]
        self._rows = [tuple(row) for row in rows]
        self.rowcount = data.get("row_count", len(self._rows))

        # Persist materializations to Iceberg catalog
        self._persist(sql, headers)

    def _persist(self, sql: str, headers: dict):
        """Detect CREATE TABLE/VIEW/DROP and persist to Iceberg catalog."""
        # Strip leading dbt comment (/* {"app": "dbt", ...} */)
        clean_sql = _COMMENT_RE.sub('', sql)

        try:
            m = _CREATE_TABLE_RE.match(clean_sql)
            if m:
                db_name, schema_name, duckdb_name = m.group(1), m.group(2), m.group(3)
                table_name = duckdb_name.removesuffix("__dbt_tmp")
                if table_name.endswith("__dbt_backup"):
                    return
                self._materialize_table(db_name, schema_name, table_name, duckdb_name, headers)
                return

            m = _CREATE_VIEW_RE.match(clean_sql)
            if m:
                db_name, schema_name, view_name = m.group(1), m.group(2), m.group(3)
                view_name = view_name.removesuffix("__dbt_tmp")
                if view_name.endswith("__dbt_backup"):
                    return
                view_sql = m.group(4).strip().rstrip(";").strip().rstrip(")")
                self._register_view(db_name, schema_name, view_name, view_sql, headers)
                return

            m = _DROP_RE.match(clean_sql)
            if m:
                obj_type = m.group(1).lower()
                db_name, schema_name, name = m.group(2), m.group(3), m.group(4)
                if name.endswith("__dbt_tmp") or name.endswith("__dbt_backup"):
                    return
                self._drop_object(db_name, schema_name, name, obj_type, headers)
                return
        except Exception as exc:
            logger.warning("Failed to persist materialization: %s", exc)

    def _materialize_table(self, db_name: str, schema_name: str, table_name: str,
                           duckdb_name: str, headers: dict):
        """Tell the backend to export Arrow from worker and persist to Iceberg."""
        duckdb_table = f'"{db_name}"."{schema_name}"."{duckdb_name}"'
        with httpx.Client(timeout=300) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/session/{self._session_id}/materialize",
                json={
                    "database": db_name,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "duckdb_table": duckdb_table,
                },
                headers=headers,
            )
            resp.raise_for_status()
        logger.info("Materialized table %s.%s.%s", db_name, schema_name, table_name)

    def _register_view(self, db_name: str, schema_name: str, view_name: str,
                       view_sql: str, headers: dict):
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/register-view",
                json={
                    "database": db_name,
                    "schema_name": schema_name,
                    "view_name": view_name,
                    "view_sql": view_sql,
                },
                headers=headers,
            )
            resp.raise_for_status()
        logger.info("Registered view %s.%s.%s", db_name, schema_name, view_name)

    def _drop_object(self, db_name: str, schema_name: str, name: str,
                     obj_type: str, headers: dict):
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/drop-object",
                json={
                    "database": db_name,
                    "schema_name": schema_name,
                    "name": name,
                    "object_type": obj_type,
                },
                headers=headers,
            )
            resp.raise_for_status()
        logger.info("Dropped %s %s.%s.%s", obj_type, db_name, schema_name, name)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        return None

    def fetchmany(self, size: int = 1):
        result = self._rows[:size]
        self._rows = self._rows[size:]
        return result

    def close(self):
        pass


class KolkhisHandle:
    """Connection handle that creates cursors for the worker session."""

    def __init__(self, backend_url: str, session_id: str, auth_token: str):
        self.backend_url = backend_url
        self.session_id = session_id
        self.auth_token = auth_token

    def cursor(self):
        return KolkhisCursor(self.backend_url, self.session_id, self.auth_token)

    def close(self):
        pass


class KolkhisConnectionManager(SQLConnectionManager):
    TYPE = "kolkhis"

    # Shared worker session across all dbt connections
    _shared_session_id: Optional[str] = None
    _cleanup_registered: bool = False

    def begin(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is True:
            return connection
        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is False:
            return connection
        connection.transaction_open = False
        return connection

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        credentials: KolkhisCredentials = connection.credentials
        headers = {"Authorization": f"Bearer {credentials.auth_token}"}

        try:
            # Create one shared worker session for the entire dbt run
            if cls._shared_session_id is None:
                with httpx.Client(timeout=300) as client:
                    # Request session creation (backend provisions worker if needed)
                    resp = client.post(
                        f"{credentials.backend_url}/api/dbt/session",
                        headers=headers,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    # If worker is provisioning, poll until ready
                    if resp.status_code == 202:
                        logger.info("Worker is being provisioned...")
                        while True:
                            time.sleep(5)
                            resp = client.get(
                                f"{credentials.backend_url}/api/dbt/session/status",
                                headers=headers,
                            )
                            resp.raise_for_status()
                            data = resp.json()
                            if data["status"] == "ready":
                                logger.info("Worker ready")
                                break
                            logger.info(data.get("message", "Provisioning worker..."))

                        # Worker is ready, retry session creation
                        resp = client.post(
                            f"{credentials.backend_url}/api/dbt/session",
                            headers=headers,
                        )
                        resp.raise_for_status()
                        data = resp.json()

                    cls._shared_session_id = data["session_id"]

                if not cls._cleanup_registered:
                    atexit.register(cls._cleanup_session, credentials)
                    cls._cleanup_registered = True

            connection.handle = KolkhisHandle(
                credentials.backend_url, cls._shared_session_id, credentials.auth_token,
            )
            connection.state = ConnectionState.OPEN

        except Exception as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise DbtRuntimeError(f"Failed to open Kolkhis connection: {exc}") from exc

        return connection

    @classmethod
    def _cleanup_session(cls, credentials: KolkhisCredentials):
        if cls._shared_session_id is None:
            return
        try:
            headers = {"Authorization": f"Bearer {credentials.auth_token}"}
            with httpx.Client(timeout=10) as client:
                client.delete(
                    f"{credentials.backend_url}/api/dbt/session/{cls._shared_session_id}",
                    headers=headers,
                )
        except Exception:
            pass
        cls._shared_session_id = None

    @classmethod
    def get_response(cls, cursor: KolkhisCursor) -> AdapterResponse:
        return AdapterResponse(_message="OK", rows_affected=cursor.rowcount)

    def cancel(self, connection: Connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except httpx.HTTPStatusError as exc:
            raise DbtRuntimeError(f"HTTP error executing SQL: {exc}") from exc
        except httpx.TransportError as exc:
            raise DbtRuntimeError(f"Connection error: {exc}") from exc
        except DbtRuntimeError:
            raise
        except Exception as exc:
            raise DbtRuntimeError(f"Error executing SQL: {exc}") from exc
