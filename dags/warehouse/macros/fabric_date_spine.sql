{% macro my_date_spine(start_date, end_date) %}
    SELECT dates = DATEADD(DAY,[value] - 1,{{ start_date }})
    FROM GENERATE_SERIES(
           1
          ,DATEDIFF(DAY
                   ,{{ start_date }}
                   ,{{ end_date }}) + 1
          ,1)
{% endmacro %}