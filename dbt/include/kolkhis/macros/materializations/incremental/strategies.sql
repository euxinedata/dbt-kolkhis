{% macro kolkhis__get_incremental_default_sql(arg_dict) %}
    {% do return(get_incremental_append_sql(arg_dict)) %}
{% endmacro %}


{% macro kolkhis__get_incremental_append_sql(arg_dict) %}
    {% set dest_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}

    INSERT INTO {{ dest_relation }}
    SELECT * FROM {{ temp_relation }}
{% endmacro %}


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
