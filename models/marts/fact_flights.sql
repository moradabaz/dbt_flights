-- models/marts/fact_flights.sql
{{
    config(
        materialized='incremental',
        partition_by=['departure_date', 'origin_airport', 'arrival_airport'],
        incremental_strategy='insert_overwrite',
        file_format='parquet'
    )
}}


WITH amadeus_facts AS (
    SELECT distinct
        flight_id,
        flight_code,
        cast(departure_date as date) as departure_date,
        departure_time,
        arrival_date,
        arrival_time,
        marketing_company_code,
        departure_airport as origin_airport,
        arrival_airport,
        total_price,
        currency,
        cast(duration_minutes / (24 * 60) as int) as duration_in_days, -- Amadeus does not have this calculated field in transformed
        duration_minutes,
        'amadeus' as source_channel,
        ingestion_dt
    FROM {{ ref('tr_amadeus_flights') }}
    {% if is_incremental() %}
    WHERE ingestion_dt >= (SELECT max(ingestion_dt) from {{ this }} WHERE source_channel = 'amadeus')
    {% endif %}
),

skyscanner_facts AS (
    SELECT distinct
        flight_id,
        flight_code,
        cast(departure_date as date) as departure_date,
        departure_time,
        arrival_date,
        arrival_time,
        marketing_company_code,
        origin_airport,
        destination_airport as arrival_airport,
        total_price,
        'USD' as currency,
        time_delta_days as duration_in_days,
        duration_in_minutes as duration_minutes,
        'skyscanner' as source_channel,
        ingestion_dt
    FROM {{ ref('tr_skyscanner_flights') }}
    {% if is_incremental() %}
    WHERE ingestion_dt >= (SELECT max(ingestion_dt) from {{ this }} WHERE source_channel = 'skyscanner')
    {% endif %}
),

unioned_flights AS (
    SELECT * FROM amadeus_facts
    UNION ALL
    SELECT * FROM skyscanner_facts
)

SELECT
    flight_id,
    flight_code,
    {{ dbt_utils.generate_surrogate_key(['marketing_company_code']) }} as airline_key,
    {{ dbt_utils.generate_surrogate_key(['origin_airport']) }} as origin_airport_key,
    departure_date,
    departure_time,
    arrival_date,
    arrival_time,
    marketing_company_code,
    origin_airport,
    arrival_airport,
    total_price,
    currency,
    duration_minutes,
    duration_in_days,
    source_channel,
    ingestion_dt
FROM unioned_flights u