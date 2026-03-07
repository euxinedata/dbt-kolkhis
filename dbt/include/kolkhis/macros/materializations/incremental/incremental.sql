{% materialization incremental, adapter='kolkhis' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {# Use a regular staging table in the same schema instead of TEMPORARY.
     Each SQL statement runs in its own DuckDB connection, so TEMPORARY tables
     are not visible across statements. A real DuckLake table persists in
     PostgreSQL metadata and is visible from any connection. #}
  {%- set temp_relation = make_intermediate_relation(target_relation) -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {% set grant_config = config.get('grants') %}

  -- Drop leftover staging table if it exists
  {{ drop_relation_if_exists(load_cached_relation(temp_relation)) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}

  {% if existing_relation is none %}
      -- First run: create the table directly
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      -- Full refresh: drop and recreate
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% else %}
      -- Incremental: build staging table in same schema, then merge/append/delete+insert
      {% do run_query(get_create_table_as_sql(False, temp_relation, sql)) %}
      {% set contract_config = config.get('contract') %}
      {% if not contract_config or not contract_config.enforced %}
        {% do adapter.expand_target_column_types(
                 from_relation=temp_relation,
                 to_relation=target_relation) %}
      {% endif %}
      {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
      {% if not dest_columns %}
        {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
      {% endif %}

      {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
      {% set strategy_arg_dict = ({
          'target_relation': target_relation,
          'temp_relation': temp_relation,
          'unique_key': unique_key,
          'dest_columns': dest_columns,
          'incremental_predicates': incremental_predicates,
      }) %}
      {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  -- Clean up staging table
  {{ drop_relation_if_exists(temp_relation) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
