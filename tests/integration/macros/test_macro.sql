{% macro test_run_operation() %}
    {% set result = run_query("SELECT 42 AS answer") %}
    {{ log("run_operation result: " ~ result.columns[0].values()[0], info=True) }}
{% endmacro %}
