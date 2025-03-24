{{ config(materialized='table') }}

select
	concat(carrier,flight_number, '-' , to_char(departure_date::date, 'YYYYMMDD-HHMI')) as flight_id,
	concat(carrier,flight_number) as flight_code,
	carrier,
	flight_number,
	departure_airport,
	arrival_airport,
	departure_date::date,
	departure_time::time,
	arrival_date::date,
	arrival_time::time,
	aircraft_code,
	EXTRACT(EPOCH FROM flight_duration::interval) / 60 AS duration_minutes,
	delay_probability::float,
	delay_result
from {{ source('sources', 'src_delayed_predictions_flights') }}
