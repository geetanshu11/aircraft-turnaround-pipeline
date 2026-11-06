select t.* from {{ ref('fact_turnarounds') }} t
join {{ ref('fact_flights') }} i on i.flight_id=t.inbound_flight_id
join {{ ref('fact_flights') }} o on o.flight_id=t.outbound_flight_id
where i.tail_number<>o.tail_number or i.carrier_id<>o.carrier_id or i.dest_id<>o.origin_id
 or not i.time_valid or not o.time_valid or i.cancelled<>0 or o.cancelled<>0 or i.diverted<>0 or o.diverted<>0
 or actual_turn_min is null or planned_turn_min is null or actual_turn_min<0 or planned_turn_min<0
 or actual_turn_min>{{ var('max_turn_minutes') }} or planned_turn_min>{{ var('max_turn_minutes') }}
 or abs((actual_turn_min-planned_turn_min)-added_delay_min)>1
