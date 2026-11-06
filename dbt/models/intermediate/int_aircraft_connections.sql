-- Port of legacy rotation_candidates, adding quarantine barriers and BTS airport IDs.
-- Whole-cohort rebuild is intentional: a historical correction can change tail
-- assignment, neighbors across months, or maximum-prior-arrival overlap checks.
with ordered as (
 select *,
  lag(flight_id) over w as previous_flight_id,
  lag(carrier_id) over w as previous_carrier_id,
  lag(dest) over w as previous_dest,
  lag(dest_id) over w as previous_dest_id,
  lag(flight_date) over w as previous_flight_date,
  lag(time_valid) over w as previous_time_valid,
  lag(cancelled) over w as previous_cancelled,
  lag(diverted) over w as previous_diverted,
  lag(scheduled_arr_utc) over w as previous_scheduled_arr_utc,
  lag(actual_arr_utc) over w as previous_actual_arr_utc,
  lag(arr_delay_min) over w as previous_arr_delay_min,
  lag(unorderable_same_day) over w as previous_unorderable_same_day,
  lag(quarantine_barrier) over w as previous_quarantine_barrier,
  max(actual_arr_utc) over(partition by tail_number
    order by coalesce(scheduled_dep_utc,flight_date::timestamp at time zone 'UTC'),flight_id
    rows between unbounded preceding and 1 preceding) as latest_prior_arrival
 from {{ ref('int_aircraft_observations') }}
 window w as (partition by tail_number order by coalesce(scheduled_dep_utc,flight_date::timestamp at time zone 'UTC'),flight_id)
), gaps as (
 select *,extract(epoch from (scheduled_dep_utc-previous_scheduled_arr_utc))/60.0 as planned_turn_min,
 extract(epoch from (actual_dep_utc-previous_actual_arr_utc))/60.0 as actual_turn_min
 from ordered
)
select *,case
 when previous_flight_id is null then 'no_previous_observation'
 when previous_carrier_id<>carrier_id then 'carrier_changed'
 when quarantine_barrier or previous_quarantine_barrier then 'quarantined_movement'
 when unorderable_same_day>0 or previous_unorderable_same_day>0 then 'unorderable_tail_day'
 when cancelled=1 or previous_cancelled=1 then 'cancelled_leg'
 when diverted=1 or previous_diverted=1 then 'diverted_leg'
 when not time_valid or not previous_time_valid then 'invalid_clock'
 when origin<>previous_dest or origin_id<>previous_dest_id then 'airport_discontinuity'
 when planned_turn_min<0 then 'schedule_overlap'
 when actual_turn_min<0 or actual_dep_utc<latest_prior_arrival then 'actual_overlap'
 when planned_turn_min>{{ var('max_turn_minutes') }} or actual_turn_min>{{ var('max_turn_minutes') }} then 'long_gap'
 else 'eligible' end as link_status
from gaps
