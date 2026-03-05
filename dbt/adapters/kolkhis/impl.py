from typing import List

from dbt.adapters.base.relation import BaseRelation
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

    def list_schemas(self, database: str) -> List[str]:
        # DuckDB's information_schema doesn't cover ATTACH'd Iceberg catalogs.
        # Use duckdb_schemas() which does enumerate them.
        db = database.strip('"')
        sql = (
            "SELECT schema_name "
            "FROM duckdb_schemas() "
            f"WHERE database_name = '{db}'"
        )
        _, table = self.execute(sql, fetch=True)
        return [row[0] for row in table]

    def check_schema_exists(self, database: str, schema: str) -> bool:
        results = self.list_schemas(database)
        return schema.lower() in [s.lower() for s in results]

    def get_columns_in_relation(self, relation):
        # DuckDB's information_schema.columns doesn't cover ATTACH'd Iceberg
        # catalogs. Use DESCRIBE which works for all DuckDB table types.
        sql = f"DESCRIBE {relation}"
        try:
            _, table = self.execute(sql, fetch=True)
        except Exception:
            return []
        return [
            self.Column.from_description(row[0], row[1])
            for row in table
        ]

    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> List[BaseRelation]:
        # DuckDB's information_schema doesn't list tables from ATTACH'd Iceberg
        # catalogs. Use SHOW TABLES FROM which does work.
        db = schema_relation.database
        schema = schema_relation.schema
        sql = f'SHOW TABLES FROM {db}."{schema}"'
        _, table = self.execute(sql, fetch=True)

        relations = []
        for row in table:
            name = row[0]
            relations.append(
                self.Relation.create(
                    database=db,
                    schema=schema,
                    identifier=name,
                    quote_policy={"database": True, "schema": True, "identifier": True},
                    type=self.Relation.get_relation_type("table"),
                )
            )
        return relations

    def valid_incremental_strategies(self):
        # DuckDB's Iceberg extension doesn't support MERGE INTO.
        return ["append", "delete+insert"]

    def debug_query(self) -> None:
        self.execute("SELECT 1 AS id")
