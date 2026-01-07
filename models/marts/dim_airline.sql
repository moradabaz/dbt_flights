{{
    config(
        materialized='table' 
    )
}}
with
    distinct_airlines as (
        select
            airline_code,
            MAX(operating_airline_code) as operating_airline_code,
            MAX(marketing_airline_name) as airline_name,
            MAX(operating_airline_name) as operating_airline_name,
            MAX(company_operation_type) as company_operation_type,
            BOOL_OR (blacklisted_in_eu) as blacklisted_in_eu,
            MAX(channel_source) as channel_source
        from {{ ref('tr_airlines_unioned') }} -- Asumo que esto es un dbt model
        where
            airline_code is not null
        GROUP BY
            airline_code
    )
select
    {{ dbt_utils.generate_surrogate_key(['airline_code']) }} as airline_key,
    airline_code,
    coalesce(
        operating_airline_code,
        airline_code
    ) as operating_airline_code,
    coalesce(airline_name, 'Unknown') as airline_name, -- Comillas simples
    coalesce(
        operating_airline_name,
        'Unknown' -- Comillas simples
    ) as operating_airline_name,
    coalesce(
        company_operation_type,
        'fully_operated' -- Comillas simples
    ) as company_operation_type,
    coalesce(blacklisted_in_eu, false) as blacklisted_in_eu,
    coalesce(channel_source, 'Unknown') as channel_source -- Comillas simples
from distinct_airlines