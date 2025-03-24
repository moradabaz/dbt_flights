{{ config(
    materialized='table'
) }}

WITH amadeus AS (
    SELECT 
        flight_code,
        number_of_stops,
        blacklisted_in_eu,
        departure_terminal
    FROM {{ ref('stg_amadeus_flights') }}
),

skyscanner AS (
    SELECT 
        flight_code,
        is_smallest_stops,
        company_operation_type,
        "isSelfTransfer",
        "isChangeAllowed",
        "isCancellationAllowed",
        "isPartiallyChangeable",
        "isPartiallyRefundable",
        "isProtectedSelfTransfer"
    FROM {{ ref('stg_skyscanner_flights') }}
)

SELECT DISTINCT
    COALESCE(a.flight_code, s.flight_code) AS flight_code,
    a.number_of_stops,
    a.blacklisted_in_eu,
    s.is_smallest_stops,
    a.departure_terminal,
    s.company_operation_type,
    s."isSelfTransfer",
    s."isChangeAllowed",
    s."isCancellationAllowed",
    s."isPartiallyChangeable",
    s."isPartiallyRefundable",
    s."isProtectedSelfTransfer"
FROM amadeus a
FULL OUTER JOIN skyscanner s
ON a.flight_code = s.flight_code
