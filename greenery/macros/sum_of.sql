{% macro sum_of(col_name, col_value) %}

SUM(CASE WHEN {{ col_name }} = '{{ col_value }}' THEN 1 ELSE 0 END)

{% endmacro %}