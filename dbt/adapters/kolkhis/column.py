from dataclasses import dataclass
from typing import Any, ClassVar, Dict

from dbt.adapters.base.column import Column


@dataclass
class KolkhisColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR",
        "INT": "INTEGER",
        "INT4": "INTEGER",
        "INT8": "BIGINT",
        "INT2": "SMALLINT",
        "FLOAT4": "FLOAT",
        "FLOAT8": "DOUBLE",
        "BOOL": "BOOLEAN",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return cls.TYPE_LABELS.get(dtype.upper(), dtype)

    @classmethod
    def string_type(cls, size: int) -> str:
        return "VARCHAR"

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        if precision is None or scale is None:
            return dtype
        return "{}({},{})".format(dtype, precision, scale)
