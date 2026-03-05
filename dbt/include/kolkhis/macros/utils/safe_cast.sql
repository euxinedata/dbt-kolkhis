{% macro kolkhis__safe_cast(field, type) %}
    TRY_CAST({{ field }} AS {{ type }})
{% endmacro %}
