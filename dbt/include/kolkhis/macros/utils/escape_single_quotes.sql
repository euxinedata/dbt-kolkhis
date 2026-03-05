{% macro kolkhis__escape_single_quotes(expression) %}
    REPLACE({{ expression }}, '''', '''''')
{% endmacro %}
