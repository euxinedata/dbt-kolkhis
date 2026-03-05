{% macro kolkhis__datediff(first_date, second_date, datepart) %}
    DATE_DIFF('{{ datepart }}', {{ first_date }}, {{ second_date }})
{% endmacro %}
