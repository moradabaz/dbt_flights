{{ config(materialized='view') }}

with flights as (
	select distinct tf.*, tci.marketing_company_code, tci.marketing_company_name, td.delay_probability, td.delay_result
	from {{ ref('tr_flights') }} tf
	join {{ ref('tr_company_info') }} tci
	on tci.flight_code = tf.flight_code
	join {{ ref('stg_delayed_prediction_flights') }} td
	on tf.flight_code = td.flight_code
)
select 
	marketing_company_name as airline,
	count(*) as total_flights,
	AVG(
        CASE 
            WHEN delay_result = 'LESS_THAN_30_MINUTES' THEN 15
            WHEN delay_result = 'BETWEEN_30_AND_60_MINUTES' THEN 45
            WHEN delay_result = 'BETWEEN_60_AND_120_MINUTES' THEN 90
            WHEN delay_result = 'OVER_120_MINUTES_OR_CANCELLED' THEN 150
            ELSE 0 
        END
    ) AS avg_delay_minutes
from flights
group by marketing_company_name
order by avg_delay_minutes desc
