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

    def __init__(self, worker_url: str, session_id: str, auth_token: str):
        self._worker_url = worker_url
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

    def __init__(self, worker_url: str, session_id: str, auth_token: str):
        self.worker_url = worker_url
        self.session_id = session_id
        self.auth_token = auth_token

    def cursor(self):
        return KolkhisCursor(self.worker_url, self.session_id, self.auth_token)

    def close(self):
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        try:
            with httpx.Client(timeout=10) as client:
                client.delete(
                    f"{self.worker_url}/session/{self.session_id}",
                    headers=headers,
                )
        except Exception:
            pass


class KolkhisConnectionManager(SQLConnectionManager):
    TYPE = "kolkhis"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        credentials: KolkhisCredentials = connection.credentials
        headers = {"Authorization": f"Bearer {credentials.auth_token}"}

        try:
            # Fetch session config from the backend
            with httpx.Client(timeout=30) as client:
                resp = client.get(
                    f"{credentials.backend_url}/api/dbt/session-config",
                    headers=headers,
                )
                resp.raise_for_status()
                config = resp.json()

            # Create a worker session
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    f"{credentials.worker_url}/session",
                    json={
                        "catalog_objects": config["catalog_objects"],
                        "s3": config["s3"],
                    },
                    headers=headers,
                )
                resp.raise_for_status()
                session_id = resp.json()["session_id"]

            connection.handle = KolkhisHandle(
                credentials.worker_url, session_id, credentials.auth_token
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
