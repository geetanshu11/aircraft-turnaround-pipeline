select flight_id as outbound_flight_id,previous_flight_id as inbound_flight_id,
 tail_number,carrier,carrier_id,origin as airport,origin_id as airport_id,flight_date,month,
 previous_actual_arr_utc as inbound_arrival_utc,actual_dep_utc as outbound_departure_utc,
 planned_turn_min::double precision,actual_turn_min::double precision,
 previous_arr_delay_min as inbound_arr_delay_min,dep_delay_min as outbound_dep_delay_min,
 previous_arr_delay_min-dep_delay_min as recovered_delay_min,dep_delay_min-previous_arr_delay_min as added_delay_min,
 previous_arr_delay_min>=15 as inbound_late,dep_delay_min>=15 as outbound_late,
 case when planned_turn_min<45 then '<45 min' when planned_turn_min<60 then '45–59 min'
 when planned_turn_min<90 then '60–89 min' when planned_turn_min<120 then '90–119 min' else '120+ min' end as planned_turn_band
from {{ ref('int_aircraft_connections') }} where link_status='eligible'
