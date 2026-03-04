from dbt.adapters.sql.impl import SQLAdapter

from dbt.adapters.kolkhis.column import KolkhisColumn
from dbt.adapters.kolkhis.connections import KolkhisConnectionManager
from dbt.adapters.kolkhis.relation import KolkhisRelation


class KolkhisAdapter(SQLAdapter):
    ConnectionManager = KolkhisConnectionManager
    Relation = KolkhisRelation
    Column = KolkhisColumn

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx: int) -> str:
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx: int) -> str:
        import agate as agate_mod

        decimals = agate_table.aggregate(agate_mod.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "BIGINT"

    @classmethod
    def convert_integer_type(cls, agate_table, col_idx: int) -> str:
        return "BIGINT"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx: int) -> str:
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx: int) -> str:
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx: int) -> str:
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx: int) -> str:
        return "TIME"
