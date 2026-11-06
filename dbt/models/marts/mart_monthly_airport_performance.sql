with departures as (
 select month,origin_id as airport_id,count(*) as scheduled_departures,sum(cancelled) as cancellations,sum(diverted) as diversions,
 count(*) filter(where cancelled=0 and dep_delay_min is not null) as measured_departures,
 count(*) filter(where cancelled=0 and dep_delay_min>=15) as late_departures,
 avg(dep_delay_min) filter(where cancelled=0) as mean_dep_delay_min,count(distinct flight_date) as observed_departure_days
 from {{ ref('fact_flights') }} group by month,origin_id
), arrivals as (
 select month,dest_id as airport_id,
 count(*) filter(where cancelled=0 and diverted=0 and arr_delay_min is not null) as measured_arrivals,
 count(*) filter(where cancelled=0 and diverted=0 and arr_delay_min<15) as ontime_arrivals
 from {{ ref('fact_flights') }} group by month,dest_id
), turns as (
 select month,airport_id,count(*) as eligible_turns,
 percentile_cont(.5) within group(order by actual_turn_min) as median_actual_turn_min,
 count(*) filter(where inbound_late) as late_inbound_turns,
 count(*) filter(where inbound_late and outbound_late) as late_inbound_late_outbound
 from {{ ref('fact_turnarounds') }} group by month,airport_id
)
select coalesce(d.month,a.month) as month,coalesce(d.airport_id,a.airport_id) as airport_id,
 d.scheduled_departures,d.cancellations,d.diversions,d.measured_departures,d.late_departures,d.mean_dep_delay_min,d.observed_departure_days,
 a.measured_arrivals,a.ontime_arrivals,100.0*d.cancellations/nullif(d.scheduled_departures,0) as cancellation_pct,
 100.0*a.ontime_arrivals/nullif(a.measured_arrivals,0) as ontime_arrival_pct,
 t.eligible_turns,t.median_actual_turn_min,t.late_inbound_turns,t.late_inbound_late_outbound,
 100.0*t.late_inbound_late_outbound/nullif(t.late_inbound_turns,0) as conditional_outbound_late_pct
from departures d full join arrivals a using(month,airport_id)
left join turns t on t.month=coalesce(d.month,a.month) and t.airport_id=coalesce(d.airport_id,a.airport_id)
