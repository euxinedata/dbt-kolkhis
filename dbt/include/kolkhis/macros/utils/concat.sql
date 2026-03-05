{% macro kolkhis__concat(fields) %}
    {{ fields | join(" || ") }}
{% endmacro %}
