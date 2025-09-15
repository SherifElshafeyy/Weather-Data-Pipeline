{% test range_values(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < -20 OR {{ column_name }} > 80
{% endtest %}
