{{ config(materialized='view') }}

SELECT 
    EXTRACT(DOW FROM departure_date) + 1 AS day_of_week,
    COUNT(*) AS total_flights
FROM {{ ref('tr_flights') }}
GROUP BY day_of_week
ORDER BY total_flights DESC
