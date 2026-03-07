import logging
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

POLL_INTERVAL = 1.0
QUERY_TIMEOUT = 300


@dataclass
class KolkhisCredentials(Credentials):
    backend_url: str = "http://localhost:8000"
    auth_token: str = ""

    @property
    def type(self) -> str:
        return "kolkhis"

    @property
    def unique_field(self) -> str:
        return self.backend_url

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("backend_url", "database", "schema")


class KolkhisCursor:
    """DB-API 2.0 cursor that submits SQL via the backend queries API.

    Each SQL statement is submitted as an independent job through
    POST /api/queries, polled until complete, then results are fetched.
    DuckLake persistence (PostgreSQL metadata + S3 data) ensures state
    is visible across ephemeral connections.
    """

    def __init__(self, backend_url: str, auth_token: str):
        self._backend_url = backend_url
        self._auth_token = auth_token
        self.description: Optional[list] = None
        self._rows: list = []
        self.rowcount: int = -1

    def _headers(self):
        return {"Authorization": f"Bearer {self._auth_token}"}

    @staticmethod
    def _quote_value(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        # String — escape single quotes
        return "'" + str(v).replace("'", "''") + "'"

    def execute(self, sql: str, bindings: Any = None):
        if bindings:
            # Inline bind parameters into the SQL string.
            # dbt seeds use %s or ? placeholders with agate row values.
            parts = sql.split("%s") if "%s" in sql else sql.split("?")
            if len(parts) == len(bindings) + 1:
                sql = "".join(
                    p + self._quote_value(b)
                    for p, b in zip(parts[:-1], bindings)
                ) + parts[-1]
            else:
                raise DbtRuntimeError(
                    f"Binding count mismatch: {len(bindings)} bindings "
                    f"for {len(parts) - 1} placeholders"
                )

        with httpx.Client(timeout=QUERY_TIMEOUT) as client:
            # Submit query
            resp = client.post(
                f"{self._backend_url}/api/queries",
                json={"sql": sql},
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()

            # DDL returns immediately
            if "ddl_message" in data:
                self.description = [
                    ("message", "VARCHAR", None, None, None, None, True)
                ]
                self._rows = [(data["ddl_message"],)]
                self.rowcount = 0
                return

            job_id = data["job_id"]

            # Poll until complete
            deadline = time.time() + QUERY_TIMEOUT
            while time.time() < deadline:
                time.sleep(POLL_INTERVAL)
                r = client.get(
                    f"{self._backend_url}/api/queries/{job_id}",
                    headers=self._headers(),
                )
                r.raise_for_status()
                job = r.json()

                if job["status"] == "failed":
                    raise DbtRuntimeError(
                        job.get("error", "Query failed")
                    )
                if job["status"] == "completed":
                    break
            else:
                raise DbtRuntimeError(
                    f"Query timed out after {QUERY_TIMEOUT}s"
                )

            # Fetch results
            r = client.get(
                f"{self._backend_url}/api/queries/{job_id}/results",
                headers=self._headers(),
            )
            r.raise_for_status()
            results = r.json()

        columns = results.get("columns") or []
        rows = results.get("rows") or []

        self.description = [
            (col, "VARCHAR", None, None, None, None, True)
            for col in columns
        ]
        self._rows = [
            tuple(row[col] for col in columns) for row in rows
        ]
        self.rowcount = results.get("total", len(self._rows))

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
    """Connection handle that creates cursors."""

    def __init__(self, backend_url: str, auth_token: str):
        self.backend_url = backend_url
        self.auth_token = auth_token

    def cursor(self):
        return KolkhisCursor(self.backend_url, self.auth_token)

    def close(self):
        pass


class KolkhisConnectionManager(SQLConnectionManager):
    TYPE = "kolkhis"

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

        credentials = connection.credentials

        try:
            connection.handle = KolkhisHandle(
                credentials.backend_url,
                credentials.auth_token,
            )
            connection.state = ConnectionState.OPEN
        except Exception as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise DbtRuntimeError(
                f"Failed to open Kolkhis connection: {exc}"
            ) from exc

        return connection

    @classmethod
    def get_response(cls, cursor: KolkhisCursor) -> AdapterResponse:
        return AdapterResponse(
            _message="OK", rows_affected=cursor.rowcount
        )

    def cancel(self, connection: Connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except httpx.HTTPStatusError as exc:
            raise DbtRuntimeError(
                f"HTTP error executing SQL: {exc}"
            ) from exc
        except httpx.TransportError as exc:
            raise DbtRuntimeError(
                f"Connection error: {exc}"
            ) from exc
        except DbtRuntimeError:
            raise
        except Exception as exc:
            raise DbtRuntimeError(
                f"Error executing SQL: {exc}"
            ) from exc
