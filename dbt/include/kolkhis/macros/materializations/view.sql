{% materialization view, adapter='kolkhis' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- If existing relation is a table, drop it first
  {% if existing_relation is not none and existing_relation.type == 'table' %}
      {% do adapter.drop_relation(existing_relation) %}
  {% endif %}

  -- CREATE OR REPLACE VIEW is atomic — no need for rename swap
  {% call statement('main') %}
      {{ get_create_view_as_sql(target_relation, sql) }}
  {% endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
