from dataclasses import dataclass, field
from typing import FrozenSet

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType


@dataclass(frozen=True, eq=False, repr=False)
class KolkhisRelation(BaseRelation):
    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {RelationType.Table, RelationType.View}
        )
    )
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({RelationType.View})
    )
    require_alias: bool = False
