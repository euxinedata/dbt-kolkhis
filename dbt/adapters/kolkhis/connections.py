import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
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

_CREATE_TABLE_RE = re.compile(
    r'^\s*CREATE\s+TABLE\s+"([^"]+)"\."([^"]+)"\s+AS\s+',
    re.IGNORECASE,
)

_CREATE_VIEW_RE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"([^"]+)"\."([^"]+)"\s+AS\s+(.+)',
    re.IGNORECASE | re.DOTALL,
)

_DROP_RE = re.compile(
    r'^\s*DROP\s+(TABLE|VIEW)\s+IF\s+EXISTS\s+"([^"]+)"\."([^"]+)"',
    re.IGNORECASE,
)


@dataclass
class KolkhisCredentials(Credentials):
    backend_url: str = "http://localhost:8000"
    worker_url: str = "http://localhost:8080"
    auth_token: str = ""

    @property
    def type(self) -> str:
        return "kolkhis"

    @property
    def unique_field(self) -> str:
        return self.worker_url

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("backend_url", "worker_url", "database", "schema")


class KolkhisCursor:
    """DB-API 2.0 style cursor that routes SQL through the worker session HTTP API."""

    def __init__(self, worker_url: str, session_id: str, auth_token: str,
                 backend_url: str = "", database: str = "kolkhis"):
        self._worker_url = worker_url
        self._session_id = session_id
        self._auth_token = auth_token
        self._backend_url = backend_url
        self._database = database
        self.description: Optional[list] = None
        self._rows: list = []
        self.rowcount: int = -1

    def execute(self, sql: str, bindings: Any = None):
        if bindings:
            raise DbtRuntimeError("Parameterized queries not supported by Kolkhis adapter")

        headers = {"Authorization": f"Bearer {self._auth_token}"}
        with httpx.Client(timeout=300) as client:
            resp = client.post(
                f"{self._worker_url}/session/{self._session_id}/query",
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
        if self._backend_url:
            self._persist(sql, headers)

    def _persist(self, sql: str, headers: dict):
        """Detect CREATE TABLE/VIEW/DROP and persist to Iceberg catalog."""
        try:
            m = _CREATE_TABLE_RE.match(sql)
            if m:
                schema_name, table_name = m.group(1), m.group(2)
                self._materialize_table(schema_name, table_name, headers)
                return

            m = _CREATE_VIEW_RE.match(sql)
            if m:
                schema_name, view_name = m.group(1), m.group(2)
                view_sql = m.group(3).strip().rstrip(";")
                self._register_view(schema_name, view_name, view_sql, headers)
                return

            m = _DROP_RE.match(sql)
            if m:
                obj_type, schema_name, name = m.group(1).lower(), m.group(2), m.group(3)
                self._drop_object(schema_name, name, obj_type, headers)
                return
        except Exception as exc:
            logger.warning("Failed to persist materialization: %s", exc)

    def _materialize_table(self, schema_name: str, table_name: str, headers: dict):
        """Export Arrow from worker, send to backend for Iceberg persistence."""
        duckdb_table = f'"{schema_name}"."{table_name}"'

        # Get Arrow bytes from worker
        with httpx.Client(timeout=300) as client:
            resp = client.post(
                f"{self._worker_url}/session/{self._session_id}/export-arrow",
                json={"table": duckdb_table},
                headers=headers,
            )
            resp.raise_for_status()
            arrow_bytes = resp.content

        # Send to backend for Iceberg write
        with httpx.Client(timeout=300) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/materialize",
                headers=headers,
                files={"arrow_data": ("data.arrow", arrow_bytes, "application/vnd.apache.arrow.stream")},
                data={
                    "database": self._database,
                    "schema_name": schema_name,
                    "table_name": table_name,
                },
            )
            resp.raise_for_status()
        logger.info("Materialized table %s.%s.%s", self._database, schema_name, table_name)

    def _register_view(self, schema_name: str, view_name: str, view_sql: str, headers: dict):
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/register-view",
                json={
                    "database": self._database,
                    "schema_name": schema_name,
                    "view_name": view_name,
                    "view_sql": view_sql,
                },
                headers=headers,
            )
            resp.raise_for_status()
        logger.info("Registered view %s.%s.%s", self._database, schema_name, view_name)

    def _drop_object(self, schema_name: str, name: str, obj_type: str, headers: dict):
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{self._backend_url}/api/dbt/drop-object",
                json={
                    "database": self._database,
                    "schema_name": schema_name,
                    "name": name,
                    "object_type": obj_type,
                },
                headers=headers,
            )
            resp.raise_for_status()
        logger.info("Dropped %s %s.%s.%s", obj_type, self._database, schema_name, name)

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

    def __init__(self, worker_url: str, session_id: str, auth_token: str,
                 backend_url: str = "", database: str = "kolkhis"):
        self.worker_url = worker_url
        self.session_id = session_id
        self.auth_token = auth_token
        self.backend_url = backend_url
        self.database = database

    def cursor(self):
        return KolkhisCursor(
            self.worker_url, self.session_id, self.auth_token,
            self.backend_url, self.database,
        )

    def close(self):
        # Don't delete the shared session — it's reused across connections
        pass


class KolkhisConnectionManager(SQLConnectionManager):
    TYPE = "kolkhis"

    # Shared worker session across all dbt connections
    _shared_session_id: Optional[str] = None
    _shared_config: Optional[dict] = None

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
                with httpx.Client(timeout=30) as client:
                    resp = client.get(
                        f"{credentials.backend_url}/api/dbt/session-config",
                        headers=headers,
                    )
                    resp.raise_for_status()
                    cls._shared_config = resp.json()

                with httpx.Client(timeout=30) as client:
                    resp = client.post(
                        f"{credentials.worker_url}/session",
                        json={
                            "catalog_objects": cls._shared_config["catalog_objects"],
                            "s3": cls._shared_config["s3"],
                            "use_databases": True,
                        },
                        headers=headers,
                    )
                    resp.raise_for_status()
                    cls._shared_session_id = resp.json()["session_id"]

            connection.handle = KolkhisHandle(
                credentials.worker_url, cls._shared_session_id, credentials.auth_token,
                credentials.backend_url, credentials.database or "kolkhis",
            )
            connection.state = ConnectionState.OPEN

        except Exception as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise DbtRuntimeError(f"Failed to open Kolkhis connection: {exc}") from exc

        return connection

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
