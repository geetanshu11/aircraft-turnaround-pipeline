-- I order every assigned leg before filtering; a cancellation or diversion can break a chain.
CREATE OR REPLACE TABLE rotation_candidates AS
WITH assigned AS (
 SELECT *,
   count(*) FILTER (WHERE scheduled_dep_utc IS NULL) OVER(PARTITION BY tail_number,flight_date) AS unorderable_same_day
 FROM flights WHERE tail_number IS NOT NULL
), ordered AS (
 SELECT *,
   lag(flight_id) OVER w AS previous_flight_id,
   lag(carrier_id) OVER w AS previous_carrier_id,
   lag(dest) OVER w AS previous_dest,
   lag(flight_date) OVER w AS previous_flight_date,
   lag(time_valid) OVER w AS previous_time_valid,
   lag(cancelled) OVER w AS previous_cancelled,
   lag(diverted) OVER w AS previous_diverted,
   lag(scheduled_arr_utc) OVER w AS previous_scheduled_arr_utc,
   lag(actual_arr_utc) OVER w AS previous_actual_arr_utc,
   lag(arr_delay_min) OVER w AS previous_arr_delay_min,
   lag(unorderable_same_day) OVER w AS previous_unorderable_same_day,
   max(actual_arr_utc) OVER(PARTITION BY tail_number ORDER BY coalesce(scheduled_dep_utc,flight_date::TIMESTAMPTZ),flight_id
       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS latest_prior_arrival
 FROM assigned WINDOW w AS (PARTITION BY tail_number ORDER BY coalesce(scheduled_dep_utc,flight_date::TIMESTAMPTZ),flight_id)
), gaps AS (
 SELECT *,
  date_diff('second',previous_scheduled_arr_utc,scheduled_dep_utc)/60.0 AS planned_turn_min,
  date_diff('second',previous_actual_arr_utc,actual_dep_utc)/60.0 AS actual_turn_min
 FROM ordered
)
SELECT *, CASE
 WHEN previous_flight_id IS NULL THEN 'no_previous_observation'
 WHEN previous_carrier_id<>carrier_id THEN 'carrier_changed'
 WHEN unorderable_same_day>0 OR previous_unorderable_same_day>0 THEN 'unorderable_tail_day'
 WHEN cancelled=1 OR previous_cancelled=1 THEN 'cancelled_leg'
 WHEN diverted=1 OR previous_diverted=1 THEN 'diverted_leg'
 WHEN NOT time_valid OR NOT previous_time_valid THEN 'invalid_clock'
 WHEN origin<>previous_dest THEN 'airport_discontinuity'
 WHEN planned_turn_min<0 THEN 'schedule_overlap'
 WHEN actual_turn_min<0 OR actual_dep_utc<latest_prior_arrival THEN 'actual_overlap'
 WHEN planned_turn_min>{max_turn} OR actual_turn_min>{max_turn} THEN 'long_gap'
 ELSE 'eligible' END AS link_status
FROM gaps;

CREATE OR REPLACE TABLE fact_turnarounds AS
SELECT flight_id AS outbound_flight_id, previous_flight_id AS inbound_flight_id,
 tail_number, carrier, carrier_id, origin AS airport, flight_date,
 previous_actual_arr_utc AS inbound_arrival_utc, actual_dep_utc AS outbound_departure_utc,
 planned_turn_min, actual_turn_min, previous_arr_delay_min AS inbound_arr_delay_min,
 dep_delay_min AS outbound_dep_delay_min,
 previous_arr_delay_min-dep_delay_min AS recovered_delay_min,
 dep_delay_min-previous_arr_delay_min AS added_delay_min,
 previous_arr_delay_min>=15 AS inbound_late,
 dep_delay_min>=15 AS outbound_late,
 CASE WHEN planned_turn_min<45 THEN '<45 min'
      WHEN planned_turn_min<60 THEN '45–59 min'
      WHEN planned_turn_min<90 THEN '60–89 min'
      WHEN planned_turn_min<120 THEN '90–119 min' ELSE '120+ min' END AS planned_turn_band
FROM rotation_candidates WHERE link_status='eligible';

CREATE OR REPLACE TABLE dim_airport AS
SELECT code AS airport, min(city) AS city, min(tz) AS timezone, min(lat) AS latitude, min(lon) AS longitude,
 count(DISTINCT airport_id) AS observed_bts_ids
FROM (
 SELECT origin AS code,origin_id AS airport_id,origin_city AS city,origin_tz AS tz,origin_lat AS lat,origin_lon AS lon FROM flights
 UNION ALL SELECT dest,dest_id,dest_city,dest_tz,dest_lat,dest_lon FROM flights
) GROUP BY code;

CREATE OR REPLACE TABLE dim_carrier AS
SELECT carrier_id,min(carrier) AS carrier,count(*) AS flights FROM flights GROUP BY carrier_id;

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT flight_date,year(flight_date) AS year,month(flight_date) AS month,
 isodow(flight_date) AS weekday,week(flight_date) AS iso_week FROM flights;

CREATE OR REPLACE TABLE mart_daily_reliability AS
SELECT flight_date,count(*) AS scheduled_flights,
 sum(cancelled) AS cancellations,sum(diverted) AS diversions,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL) AS measured_arrivals,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min>=15) AS late_arrivals,
 avg(arr_delay_min) FILTER(WHERE cancelled=0 AND diverted=0) AS mean_arr_delay_min,
 quantile_cont(arr_delay_min,0.9) FILTER(WHERE cancelled=0 AND diverted=0) AS p90_arr_delay_min,
 100.0*sum(cancelled)/count(*) AS cancellation_pct,
 100.0*count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min<15)
  /nullif(count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL),0) AS ontime_arrival_pct
FROM flights GROUP BY flight_date;

CREATE OR REPLACE TABLE mart_airport_daily AS
SELECT flight_date,origin AS airport,count(*) AS scheduled_departures,sum(cancelled) AS cancellations,
 count(*) FILTER(WHERE cancelled=0 AND dep_delay_min IS NOT NULL) AS measured_departures,
 count(*) FILTER(WHERE cancelled=0 AND dep_delay_min>=15) AS late_departures,
 avg(dep_delay_min) FILTER(WHERE cancelled=0) AS mean_dep_delay_min
FROM flights GROUP BY flight_date,origin;

CREATE OR REPLACE TABLE mart_routes AS
SELECT origin,dest,count(*) AS scheduled_flights,sum(cancelled) AS cancellations,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL) AS measured_arrivals,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min>=15) AS late_arrivals,
 avg(arr_delay_min) FILTER(WHERE cancelled=0 AND diverted=0) AS mean_arr_delay_min
FROM flights GROUP BY origin,dest;

CREATE OR REPLACE TABLE mart_turnaround_airports AS
SELECT airport,count(*) AS eligible_turns,
 median(planned_turn_min) AS median_planned_turn_min,median(actual_turn_min) AS median_actual_turn_min,
 count(*) FILTER(WHERE inbound_late) AS late_inbound_turns,
 count(*) FILTER(WHERE inbound_late AND outbound_late) AS late_inbound_late_outbound,
 count(*) FILTER(WHERE inbound_late AND NOT outbound_late) AS recovered_to_ontime,
 100.0*count(*) FILTER(WHERE inbound_late AND outbound_late)
 /nullif(count(*) FILTER(WHERE inbound_late),0) AS conditional_outbound_late_pct,
 median(recovered_delay_min) FILTER(WHERE inbound_late) AS median_recovery_min
FROM fact_turnarounds GROUP BY airport;

CREATE OR REPLACE TABLE mart_turn_bands AS
SELECT planned_turn_band,count(*) AS eligible_turns,count(*) FILTER(WHERE inbound_late) AS late_inbound_turns,
 count(*) FILTER(WHERE inbound_late AND outbound_late) AS late_inbound_late_outbound,
 100.0*count(*) FILTER(WHERE inbound_late AND outbound_late)
 /nullif(count(*) FILTER(WHERE inbound_late),0) AS conditional_outbound_late_pct,
 median(recovered_delay_min) FILTER(WHERE inbound_late) AS median_recovery_min
FROM fact_turnarounds GROUP BY planned_turn_band;

CREATE OR REPLACE TABLE mart_reported_causes AS
SELECT 'Carrier' AS cause, sum(carrier_delay_min) AS reported_minutes,count(carrier_delay_min) AS nonnull_records FROM flights
UNION ALL SELECT 'Weather',sum(weather_delay_min),count(weather_delay_min) FROM flights
UNION ALL SELECT 'National aviation system',sum(nas_delay_min),count(nas_delay_min) FROM flights
UNION ALL SELECT 'Security',sum(security_delay_min),count(security_delay_min) FROM flights
UNION ALL SELECT 'Late-arriving aircraft',sum(late_aircraft_delay_min),count(late_aircraft_delay_min) FROM flights;

CREATE OR REPLACE TABLE mart_link_coverage AS
SELECT link_status,count(*) AS candidate_records FROM rotation_candidates GROUP BY link_status;
