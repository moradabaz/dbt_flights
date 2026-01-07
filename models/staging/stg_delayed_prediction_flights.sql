{{ config(materialized='table') }}

select distinct
	concat(concat(carrier, flight_number), '-', date_format(date_parse(departure_date, '%Y-%m-%d'), '%Y%m%d'), '-', date_format(date_parse(departure_time, '%H:%i:%s'), '%H%i')) as flight_id,
	concat(carrier,flight_number) as flight_code,
	carrier as marketing_carrier_code,
	flight_number,
	departure_airport as origin_airport_code,
	arrival_airport as destination_airport_code,
    date_format(date_parse(departure_date, '%Y-%m-%d'), '%Y-%m-%d')	as departure_date,
   	date_format(date_parse(departure_time, '%H:%i:%s'), '%H:%i:%s') as departure_time,
    date_format(date_parse(arrival_date, '%Y-%m-%d'), '%Y-%m-%d') as arrival_date,
    date_format(date_parse(arrival_time, '%H:%i:%s'), '%H:%i:%s') as arrival_time,
	aircraft_code,
	(coalesce(try_cast(regexp_extract(flight_duration, '(\d+)H', 1) as integer), 0) * 60 + 
     coalesce(try_cast(regexp_extract(flight_duration, '(\d+)M', 1) as integer), 0)) as duration_minutes,
	delay_probability,
	delay_result,
	ingestion_dt as ingestion_timestamp
from {{ source('sources', 'src_delayed_predictions_flights') }}