select flight_id,flight_date,month,carrier_id,origin_id,dest_id,cancelled,diverted,cancellation_code,
 dep_delay_min,arr_delay_min,carrier_delay_min,weather_delay_min,nas_delay_min,security_delay_min,late_aircraft_delay_min,
 case when cancelled=1 then 'cancelled' when diverted=1 then 'diverted' when arr_delay_min is null then 'arrival_unmeasured'
 when arr_delay_min>=15 then 'late_arrival' else 'on_time_arrival' end as arrival_outcome
from {{ ref('fact_flights') }}
