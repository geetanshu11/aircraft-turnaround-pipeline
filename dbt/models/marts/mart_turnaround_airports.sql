-- Ported from the original analytical SQL.
SELECT airport,count(*) AS eligible_turns,
 percentile_cont(0.5) WITHIN GROUP(ORDER BY planned_turn_min) AS median_planned_turn_min,percentile_cont(0.5) WITHIN GROUP(ORDER BY actual_turn_min) AS median_actual_turn_min,
 count(*) FILTER(WHERE inbound_late) AS late_inbound_turns,
 count(*) FILTER(WHERE inbound_late AND outbound_late) AS late_inbound_late_outbound,
 count(*) FILTER(WHERE inbound_late AND NOT outbound_late) AS recovered_to_ontime,
 100.0*count(*) FILTER(WHERE inbound_late AND outbound_late)
 /nullif(count(*) FILTER(WHERE inbound_late),0) AS conditional_outbound_late_pct,
 percentile_cont(0.5) WITHIN GROUP(ORDER BY recovered_delay_min) FILTER(WHERE inbound_late) AS median_recovery_min
FROM {{ ref('fact_turnarounds') }} GROUP BY airport
