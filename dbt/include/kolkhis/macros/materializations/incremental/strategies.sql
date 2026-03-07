{% macro kolkhis__get_incremental_default_sql(arg_dict) %}
    {% do return(get_incremental_append_sql(arg_dict)) %}
{% endmacro %}


{% macro kolkhis__get_incremental_append_sql(arg_dict) %}
    {% set dest_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}

    INSERT INTO {{ dest_relation }}
    SELECT * FROM {{ temp_relation }}
{% endmacro %}


{% macro kolkhis__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
            {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
            {% set unique_key_match = equals(source_unique_key, target_unique_key) | trim %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({% for col in dest_columns -%}
            DBT_INTERNAL_SOURCE.{{ adapter.quote(col.name) }}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %})

{%- endmacro %}


{% macro kolkhis__get_incremental_delete_insert_sql(arg_dict) %}
    {% set dest_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}

    {% if unique_key is string %}
        {% set unique_keys = [unique_key] %}
    {% else %}
        {% set unique_keys = unique_key %}
    {% endif %}

    DELETE FROM {{ dest_relation }}
    WHERE (
        {% for key in unique_keys %}
            {{ key }}
            {%- if not loop.last %}, {% endif %}
        {% endfor %}
    ) IN (
        SELECT
            {% for key in unique_keys %}
                {{ key }}
                {%- if not loop.last %}, {% endif %}
            {% endfor %}
        FROM {{ temp_relation }}
    );

    INSERT INTO {{ dest_relation }}
    SELECT * FROM {{ temp_relation }}
{% endmacro %}
