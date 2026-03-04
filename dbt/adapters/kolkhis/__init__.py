from dbt.adapters.base import AdapterPlugin

from dbt.adapters.kolkhis.column import KolkhisColumn
from dbt.adapters.kolkhis.connections import KolkhisConnectionManager, KolkhisCredentials
from dbt.adapters.kolkhis.impl import KolkhisAdapter
from dbt.adapters.kolkhis.relation import KolkhisRelation
from dbt.include.kolkhis import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=KolkhisAdapter,
    credentials=KolkhisCredentials,
    include_path=PACKAGE_PATH,
)
