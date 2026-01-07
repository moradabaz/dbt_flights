{{ config(materialized='table') }}


with skyscanner_origins as (
    select distinct
        origin_airport as airport_code,
        departure_city as city,
        origin_country as country,
        'skyscanner' as source_priority
    from {{ ref('tr_skyscanner_flights') }}
    where origin_airport is not null
),

skyscanner_destinations as (
    select distinct
        destination_airport as airport_code,
        destination_name as airport_name,
        destination_city as city,
        destination_country as country,
        'skyscanner' as source_priority
    from {{ ref('tr_skyscanner_flights') }}
    where destination_airport is not null
),

amadeus_origins as (
    select distinct
        departure_airport as airport_code,
        cast(null as varchar) as city,
        cast(null as varchar) as country,
        'amadeus' as source_priority
    from {{ ref('tr_amadeus_flights') }}
    where departure_airport is not null
),

amadeus_destinations as (
    select distinct
        arrival_airport as airport_code,
        cast(null as varchar) as city,
        cast(null as varchar) as country,
        'amadeus' as source_priority
    from {{ ref('tr_amadeus_flights') }}
    where arrival_airport is not null
),

unioned_airports as (
    select * from skyscanner_origins
    union all
    select * from skyscanner_destinations
    union all
    select * from amadeus_origins
    union all
    select * from amadeus_destinations
),

aggregated_airports as (
    select
        airport_code,
        -- Prioritize Skyscanner values (which are not null) over Amadeus (nulls)
        MAX(city) as city,
        MAX(country) as country,
        -- Keep track of sources (optional, but follows pattern)
        MAX(source_priority) as distinct_source
    from unioned_airports
    group by airport_code
)

select
    {{ dbt_utils.generate_surrogate_key(['airport_code']) }} as airport_key,
    airport_code,
    coalesce(city, 'Unknown') as city,
    coalesce(country, 'Unknown') as country
from aggregated_airports