{% test range_values(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE run_id = '{{ var("run_id") }}'
  AND ({{ column_name }} < -20 OR {{ column_name }} > 80)
{% endtest %}
