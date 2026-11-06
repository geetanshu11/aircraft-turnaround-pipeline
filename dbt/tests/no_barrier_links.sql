select flight_id from {{ ref('int_aircraft_connections') }} where link_status='eligible'
 and (quarantine_barrier or previous_quarantine_barrier or unorderable_same_day>0 or previous_unorderable_same_day>0)
