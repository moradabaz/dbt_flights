{{ config(materialized='table') }}

select distinct
	concat(concat(carrier, flight_number), '-', date_format(date_parse(departure_time, '%Y-%m-%dT%H:%i:%s'), '%Y%m%d-%H%i')) as flight_id,
    concat(carrier, flight_number) as flight_code,
	departure_airport as origin_airport_code,
	date_format(date_parse(departure_time, '%Y-%m-%dT%H:%i:%s'), '%Y-%m-%d') as departure_date,
	date_format(date_parse(departure_time, '%Y-%m-%dT%H:%i:%s'), '%H:%i:%s') as departure_time,
	arrival_airport as destination_airport_code,
    date_format(date_parse(arrival_time, '%Y-%m-%dT%H:%i:%s'), '%Y-%m-%d') as arrival_date,
    date_format(date_parse(arrival_time, '%Y-%m-%dT%H:%i:%s'), '%H:%i:%s') as arrival_time,
	(coalesce(try_cast(regexp_extract(flight_duration, '(\d+)H', 1) as integer), 0) * 60 + 
     coalesce(try_cast(regexp_extract(flight_duration, '(\d+)M', 1) as integer), 0)) as duration_minutes,
	carrier as marketing_carrier_code,
	company_code as operating_carrier_code,
	aircraft_code,
	flight_number,
	cast(total_price as double) as price_amount,
	number_of_stops,
	blacklisted_in_eu,
	departure_terminal,
	currency as currency_code,
	ingestion_dt as ingestion_timestamp
from {{ source('sources', 'src_amadeus_flights') }}