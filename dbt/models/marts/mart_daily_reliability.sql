-- Ported from the original analytical SQL.
SELECT flight_date,count(*) AS scheduled_flights,
 sum(cancelled) AS cancellations,sum(diverted) AS diversions,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL) AS measured_arrivals,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min>=15) AS late_arrivals,
 avg(arr_delay_min) FILTER(WHERE cancelled=0 AND diverted=0) AS mean_arr_delay_min,
 percentile_cont(0.9) WITHIN GROUP(ORDER BY arr_delay_min) FILTER(WHERE cancelled=0 AND diverted=0) AS p90_arr_delay_min,
 100.0*sum(cancelled)/count(*) AS cancellation_pct,
 100.0*count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min<15)
  /nullif(count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL),0) AS ontime_arrival_pct
FROM {{ ref('fact_flights') }} GROUP BY flight_date
