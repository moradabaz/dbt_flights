{{ config(materialized='view') }}

select
    -- Common and Amadeus specific columns
    flight_id,
    flight_code,
    departure_airport,
    departure_date,
    departure_time,
    arrival_airport,
    arrival_date,
    arrival_time,
    duration_minutes,
    marketing_company_code,
    operating_company_code,
    aircraft_code,
    flight_number,
    total_price,
    number_of_stops,
    blacklisted_in_eu,
    departure_terminal,
    currency,
    ingestion_dt,

-- Channel source
'amadeus' as channel_source,

-- Skyscanner specific columns (set to NULL)


cast(null as varchar) as departure_city,
    cast(null as varchar) as origin_airport,
    cast(null as varchar) as origin_country,
    cast(null as varchar) as destination_name,
    cast(null as varchar) as destination_airport,
    cast(null as varchar) as destination_country,
    cast(null as integer) as time_delta_days,
    cast(null as varchar) as destination_city,
    cast(null as boolean) as is_smallest_stops,
    cast(null as integer) as duration_in_minutes,
    cast(null as varchar) as company_operation_type,
    cast(null as varchar) as marketing_company_name,
    cast(null as varchar) as operating_company_name,
    cast(null as boolean) as is_self_transfer,
    cast(null as boolean) as is_change_allowed,
    cast(null as boolean) as is_cancellation_allowed,
    cast(null as boolean) as is_partially_changeable,
    cast(null as boolean) as is_partially_refundable,
    cast(null as boolean) as is_protected_self_transfer

from {{ ref('stg_amadeus_flights') }}