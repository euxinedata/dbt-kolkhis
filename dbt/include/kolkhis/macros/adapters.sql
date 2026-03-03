{% macro kolkhis__current_timestamp() %}
    now()
{% endmacro %}


{% macro kolkhis__list_schemas(database) %}
    {% set sql %}
        select schema_name
        from information_schema.schemata
        where catalog_name = current_database()
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}


{% macro kolkhis__list_relations_without_caching(schema_relation) %}
    {% set sql %}
        select
            '{{ schema_relation.database }}' as database,
            table_name as name,
            table_schema as schema,
            case table_type
                when 'BASE TABLE' then 'table'
                when 'VIEW' then 'view'
                else table_type
            end as type
        from information_schema.tables
        where table_schema = '{{ schema_relation.schema }}'
          and table_catalog = current_database()
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}


{% macro kolkhis__get_columns_in_relation(relation) %}
    {% set sql %}
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        from information_schema.columns
        where table_name = '{{ relation.identifier }}'
          and table_schema = '{{ relation.schema }}'
        order by ordinal_position
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}


{% macro kolkhis__create_table_as(temporary, relation, sql) %}
    create {% if temporary -%}temporary {%- endif %} table
        {{ relation.include(database=false) }}
    as (
        {{ sql }}
    )
{% endmacro %}


{% macro kolkhis__create_view_as(relation, sql) %}
    create or replace view {{ relation.include(database=false) }} as (
        {{ sql }}
    )
{% endmacro %}


{% macro kolkhis__drop_relation(relation) %}
    {%- call statement('drop_relation', auto_begin=False) -%}
        {% if relation.type == 'view' %}
            drop view if exists {{ relation.include(database=false) }}
        {% elif relation.type == 'table' %}
            drop table if exists {{ relation.include(database=false) }}
        {% endif %}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__rename_relation(from_relation, to_relation) %}
    {%- call statement('rename_relation') -%}
        {% if from_relation.type == 'view' %}
            alter view {{ from_relation.include(database=false) }}
            rename to {{ to_relation.include(database=false, schema=false) }}
        {% else %}
            alter table {{ from_relation.include(database=false) }}
            rename to {{ to_relation.include(database=false, schema=false) }}
        {% endif %}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__create_schema(relation) %}
    {%- call statement('create_schema') -%}
        create schema if not exists {{ relation.without_identifier().include(database=false) }}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__drop_schema(relation) %}
    {%- call statement('drop_schema') -%}
        drop schema if exists {{ relation.without_identifier().include(database=false) }} cascade
    {%- endcall -%}
{% endmacro %}
