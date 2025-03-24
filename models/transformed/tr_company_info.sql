{{ config(
    materialized='table'
) }}

WITH skyscanner AS (
    SELECT 
        flight_code,
        marketing_company_code,
        marketing_company_name,
        operating_company_code,
        operating_company_name
    FROM {{ ref('stg_skyscanner_flights') }}
),

amadeus AS (
    SELECT 
        flight_code,
        marketing_company_code,
        operating_company_code,
        aircraft_code
    FROM {{ ref('stg_amadeus_flights') }}
)

SELECT DISTINCT
    s.flight_code,
    s.marketing_company_code,
    s.marketing_company_name,
    s.operating_company_code,
    s.operating_company_name,
    a.aircraft_code 
FROM skyscanner s
LEFT JOIN amadeus a
ON s.flight_code = a.flight_code 
AND s.marketing_company_code = a.marketing_company_code
AND s.operating_company_code = a.operating_company_code