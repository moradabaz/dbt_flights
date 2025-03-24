{{ config(materialized='table') }}

select
	concat(carrier,flight_number, '-' , to_char(departure_time::date, 'YYYYMMDD-HHMI')) as flight_id,
	concat(carrier,flight_number) as flight_code,
	departure_airport,
	departure_time::timestamp,
	arrival_airport,
	arrival_time::timestamp,
	EXTRACT(EPOCH FROM flight_duration::interval) / 60 AS duration_minutes,
	carrier as marketing_company_code,
	company_code as operating_company_code,
	aircraft_code,
	flight_number,
	total_price::float,
	number_of_stops,
	blacklisted_in_eu,
	departure_terminal,
	currency
from {{ source('sources', 'src_amadeus_flights') }}

