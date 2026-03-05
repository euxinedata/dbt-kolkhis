{% macro kolkhis__split_part(string_text, delimiter_text, part_number) %}
    STRING_SPLIT({{ string_text }}, {{ delimiter_text }})[{{ part_number }}]
{% endmacro %}
