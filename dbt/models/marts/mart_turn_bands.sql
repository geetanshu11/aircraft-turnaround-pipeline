-- Ported from the original analytical SQL.
SELECT planned_turn_band,count(*) AS eligible_turns,count(*) FILTER(WHERE inbound_late) AS late_inbound_turns,
 count(*) FILTER(WHERE inbound_late AND outbound_late) AS late_inbound_late_outbound,
 100.0*count(*) FILTER(WHERE inbound_late AND outbound_late)
 /nullif(count(*) FILTER(WHERE inbound_late),0) AS conditional_outbound_late_pct,
 percentile_cont(0.5) WITHIN GROUP(ORDER BY recovered_delay_min) FILTER(WHERE inbound_late) AS median_recovery_min
FROM {{ ref('fact_turnarounds') }} GROUP BY planned_turn_band
