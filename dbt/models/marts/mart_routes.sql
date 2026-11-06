-- Ported from the original analytical SQL.
SELECT origin,dest,count(*) AS scheduled_flights,sum(cancelled) AS cancellations,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL) AS measured_arrivals,
 count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min>=15) AS late_arrivals,
 avg(arr_delay_min) FILTER(WHERE cancelled=0 AND diverted=0) AS mean_arr_delay_min
FROM {{ ref('fact_flights') }} GROUP BY origin,dest
