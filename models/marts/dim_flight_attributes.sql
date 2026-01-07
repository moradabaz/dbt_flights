{{ config(materialized='table') }}


with amadeus_flags as (
    select distinct
        flight_code,
        blacklisted_in_eu,
        -- Set Skyscanner flags to null/false
        cast(null as boolean) as is_smallest_stops,
        cast(null as boolean) as is_self_transfer,
        cast(null as boolean) as is_change_allowed,
        cast(null as boolean) as is_cancellation_allowed,
        cast(null as boolean) as is_partially_changeable,
        cast(null as boolean) as is_partially_refundable,
        cast(null as boolean) as is_protected_self_transfer,
        'amadeus' as source_priority
    from {{ ref('tr_amadeus_flights') }}
    where flight_code is not null
),

skyscanner_flags as (
    select distinct
        flight_code,
        -- Set Amadeus flags to null/false
        cast(null as boolean) as blacklisted_in_eu,
        is_smallest_stops,
        is_self_transfer,
        is_change_allowed,
        is_cancellation_allowed,
        is_partially_changeable,
        is_partially_refundable,
        is_protected_self_transfer,
        'skyscanner' as source_priority
    from {{ ref('tr_skyscanner_flights') }}
    where flight_code is not null
),

unioned_flags as (
    select * from amadeus_flags
    union all
    select * from skyscanner_flags
),

aggregated_flags as (
    select
        flight_code,
        -- Aggregate flags. Assuming if it's true in any recurrence/source, it's a property of the flight code.
        -- Using max() for booleans works in many SQL dialects (True > False), otherwise bool_or()
        BOOL_OR(blacklisted_in_eu) as blacklisted_in_eu,
        BOOL_OR(is_smallest_stops) as is_smallest_stops,
        BOOL_OR(is_self_transfer) as is_self_transfer,
        BOOL_OR(is_change_allowed) as is_change_allowed,
        BOOL_OR(is_cancellation_allowed) as is_cancellation_allowed,
        BOOL_OR(is_partially_changeable) as is_partially_changeable,
        BOOL_OR(is_partially_refundable) as is_partially_refundable,
        BOOL_OR(is_protected_self_transfer) as is_protected_self_transfer
    from unioned_flags
    group by flight_code
)

select
    {{ dbt_utils.generate_surrogate_key(['flight_code']) }} as flight_attributes_key,
    flight_code,
    coalesce(blacklisted_in_eu, false) as blacklisted_in_eu,
    coalesce(is_smallest_stops, false) as is_smallest_stops,
    coalesce(is_self_transfer, false) as is_self_transfer,
    coalesce(is_change_allowed, false) as is_change_allowed,
    coalesce(is_cancellation_allowed, false) as is_cancellation_allowed,
    coalesce(is_partially_changeable, false) as is_partially_changeable,
    coalesce(is_partially_refundable, false) as is_partially_refundable,
    coalesce(is_protected_self_transfer, false) as is_protected_self_transfer
from aggregated_flags