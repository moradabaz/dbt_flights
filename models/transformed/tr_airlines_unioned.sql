{{ config(materialized='view') }}


with amadeus_data as (
    select distinct
        marketing_company_code as airline_code,
        marketing_company_code as operating_airline_code,
        cast(null as varchar) as marketing_airline_name,
        cast(null as varchar) as operating_airline_name,
        blacklisted_in_eu,
        cast(null as varchar) as company_operation_type,
        'amadeus' as channel_source
    from {{ ref('stg_amadeus_flights') }}
),

skyscanner_data as (
    select distinct
        marketing_company_code as airline_code,
        operating_company_code as operating_airline_code,
        marketing_company_name as marketing_airline_name,
        operating_company_name as operating_airline_name,
        cast(null as boolean) as blacklisted_in_eu,
        company_operation_type,
        'skyscanner' as channel_source
    from {{ ref('stg_skyscanner_flights') }}
)

select * from amadeus_data union all select * from skyscanner_data