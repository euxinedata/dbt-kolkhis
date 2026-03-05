{% macro kolkhis__list_schemas(database) %}
    {# DuckDB's information_schema doesn't cover ATTACH'd Iceberg catalogs #}
    {% set sql %}
        SELECT schema_name
        FROM duckdb_schemas()
        WHERE database_name = '{{ database }}'
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}


{% macro kolkhis__list_relations_without_caching(schema_relation) %}
    {# DuckDB's information_schema doesn't list Iceberg tables, use SHOW TABLES #}
    {% set sql %}
        SHOW TABLES FROM {{ schema_relation.database }}."{{ schema_relation.schema }}"
    {% endset %}
    {% set results = run_query(sql) %}
    {% set relations = [] %}
    {% for row in results %}
        {% do relations.append({'database': schema_relation.database, 'name': row[0], 'schema': schema_relation.schema, 'type': 'table'}) %}
    {% endfor %}
    {{ return(relations) }}
{% endmacro %}


{% macro kolkhis__get_columns_in_relation(relation) %}
    {# information_schema.columns doesn't work for ATTACH'd Iceberg catalogs.
       Use DESCRIBE which returns column_name, column_type, null, key, default, extra. #}
    {% set sql %}
        DESCRIBE {{ relation }}
    {% endset %}
    {% set result = run_query(sql) %}
    {% set columns = [] %}
    {% for row in result %}
        {% do columns.append(api.Column.from_description(row['column_name'], row['column_type'])) %}
    {% endfor %}
    {% do return(columns) %}
{% endmacro %}


{% macro kolkhis__create_table_as(temporary, relation, sql) %}
    CREATE {% if temporary -%}TEMPORARY {%- endif %} TABLE
        {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    AS (
        {{ sql }}
    )
{% endmacro %}


{% macro kolkhis__create_view_as(relation, sql) %}
    CREATE OR REPLACE VIEW {{ relation }} AS (
        {{ sql }}
    )
{% endmacro %}


{% macro kolkhis__drop_relation(relation) %}
    {%- call statement('drop_relation', auto_begin=False) -%}
        {% if relation.type == 'view' %}
            DROP VIEW IF EXISTS {{ relation }}
        {% elif relation.type == 'table' %}
            DROP TABLE IF EXISTS {{ relation }}
        {% endif %}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__rename_relation(from_relation, to_relation) %}
    {# Iceberg tables don't support ALTER TABLE RENAME.
       Only views can be renamed. Tables should use drop+create instead. #}
    {%- call statement('rename_relation') -%}
        {% if from_relation.type == 'view' %}
            ALTER VIEW {{ from_relation }}
            RENAME TO {{ to_relation.include(database=false, schema=false) }}
        {% else %}
            {{ exceptions.raise_compiler_error("Renaming tables is not supported on Iceberg catalogs. Use drop+create instead.") }}
        {% endif %}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__create_schema(relation) %}
    {%- call statement('create_schema') -%}
        CREATE SCHEMA IF NOT EXISTS {{ relation.without_identifier() }}
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__drop_schema(relation) %}
    {%- call statement('drop_schema') -%}
        DROP SCHEMA IF EXISTS {{ relation.without_identifier() }} CASCADE
    {%- endcall -%}
{% endmacro %}


{% macro kolkhis__truncate_relation(relation) %}
    {%- call statement('truncate_relation') -%}
        DELETE FROM {{ relation }} WHERE true
    {%- endcall -%}
{% endmacro %}
