-- I keep denominators visible whenever I compare rates.
SELECT airport,eligible_turns,late_inbound_turns,conditional_outbound_late_pct,median_recovery_min
FROM mart_turnaround_airports WHERE late_inbound_turns>=100 ORDER BY conditional_outbound_late_pct DESC;

-- This is an observed sequence, not proof of a causal delay chain.
SELECT f.flight_date,f.tail_number,f.carrier,f.flight_number,f.origin,f.dest,
       f.scheduled_dep_utc,f.actual_dep_utc,f.actual_arr_utc,f.dep_delay_min,f.arr_delay_min,
       r.link_status,r.planned_turn_min,r.actual_turn_min
FROM flights f LEFT JOIN rotation_candidates r USING(flight_id)
WHERE f.tail_number='N104NN' ORDER BY f.scheduled_dep_utc;

-- I inspect excluded rows before interpreting turnaround findings.
SELECT time_status,count(*) AS records FROM flights GROUP BY time_status ORDER BY records DESC;
SELECT * FROM mart_link_coverage ORDER BY candidate_records DESC;

-- The checkpoint identifies the active full snapshot for each month.
SELECT c.partition_month,v.version_id,v.source_sha256,v.raw_rows,v.accepted_rows,v.received_at
FROM checkpoints c JOIN partition_versions v USING(version_id) ORDER BY c.partition_month;

-- This checks what was replaced when an already loaded source month changed.
SELECT * FROM partition_activations ORDER BY committed_at;
SELECT change_type,count(*) FROM flight_changes GROUP BY change_type;
