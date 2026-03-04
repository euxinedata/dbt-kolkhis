from dataclasses import dataclass, field
from typing import FrozenSet, Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import Policy, RelationType


@dataclass
class KolkhisQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class KolkhisIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class KolkhisRelation(BaseRelation):
    quote_character: str = '"'
    quote_policy: Policy = field(default_factory=lambda: KolkhisQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: KolkhisIncludePolicy())
    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({RelationType.Table, RelationType.View})
    )
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({RelationType.View})
    )
    require_alias: bool = False
