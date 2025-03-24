{{ config(materialized='table') }}


select 
	concat(flight_id, '-AMA') as flight_id,
	flight_code ,
	departure_airport as origin,
	departure_time::date as departure_date,
	departure_time::time as departure_time,
	arrival_airport as destination,
	arrival_time::date as arrival_date,
	arrival_time::time as arrival_time,
	duration_minutes,
	total_price,
	'AMADEUS' as agency
from  {{ ref('stg_amadeus_flights') }}
union
select 
	concat(flight_id, '-SKY') as flight_id,
	flight_code,
	origin_airport as origin,
	departure_time::date as departure_date,
	departure_time::time as departure_time,
	destination_airport as destination,
	arrival_time::date as arrival_date,
	arrival_time::time as arrival_time,
	duration_in_minutes,
	total_price,
	'SKYSCANNER' as agency
from  {{ ref('stg_skyscanner_flights') }} 
