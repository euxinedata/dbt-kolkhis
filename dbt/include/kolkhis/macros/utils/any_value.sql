{% macro kolkhis__any_value(expression) %}
    ANY_VALUE({{ expression }})
{% endmacro %}
