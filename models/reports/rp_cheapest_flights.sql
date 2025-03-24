{{ config(materialized='view') }}

SELECT 
    departure_date,
    origin,
    destination,
    MIN(total_price) AS lowest_price,
    COUNT(*) AS total_flights
FROM {{ ref('tr_flights') }}
GROUP BY departure_date, origin, destination
ORDER BY departure_date ASC
