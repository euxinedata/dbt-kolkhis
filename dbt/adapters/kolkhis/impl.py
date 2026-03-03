from dbt.adapters.sql.impl import SQLAdapter

from dbt.adapters.kolkhis.connections import KolkhisConnectionManager


class KolkhisAdapter(SQLAdapter):
    ConnectionManager = KolkhisConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"
