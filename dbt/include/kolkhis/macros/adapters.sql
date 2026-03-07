{% macro kolkhis__list_schemas(database) %}
    {% set sql %}
        SELECT schema_name
        FROM duckdb_schemas()
        WHERE database_name = '{{ database }}'
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}


{% macro kolkhis__list_relations_without_caching(schema_relation) %}
    {% set sql %}
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = '{{ schema_relation.database }}'
          AND table_schema = '{{ schema_relation.schema }}'
    {% endset %}
    {% set results = run_query(sql) %}
    {% set relations = [] %}
    {% for row in results %}
        {% if row['table_type'] == 'VIEW' %}
            {% set rel_type = 'view' %}
        {% else %}
            {% set rel_type = 'table' %}
        {% endif %}
        {% do relations.append({'database': schema_relation.database, 'name': row['table_name'], 'schema': schema_relation.schema, 'type': rel_type}) %}
    {% endfor %}
    {{ return(relations) }}
{% endmacro %}


{% macro kolkhis__get_columns_in_relation(relation) %}
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
    {%- call statement('rename_relation') -%}
        {% if from_relation.type == 'view' %}
            ALTER VIEW {{ from_relation }}
            RENAME TO {{ to_relation.include(database=false, schema=false) }}
        {% else %}
            ALTER TABLE {{ from_relation }}
            RENAME TO {{ to_relation.include(database=false, schema=false) }}
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
