-- Ported from the original analytical SQL.
SELECT flight_date,origin AS airport,count(*) AS scheduled_departures,sum(cancelled) AS cancellations,
 count(*) FILTER(WHERE cancelled=0 AND dep_delay_min IS NOT NULL) AS measured_departures,
 count(*) FILTER(WHERE cancelled=0 AND dep_delay_min>=15) AS late_departures,
 avg(dep_delay_min) FILTER(WHERE cancelled=0) AS mean_dep_delay_min
FROM {{ ref('fact_flights') }} GROUP BY flight_date,origin
