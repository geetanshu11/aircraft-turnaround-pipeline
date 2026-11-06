select flight_id from {{ ref('fact_flights') }}
where origin_lat not between -90 and 90 or dest_lat not between -90 and 90
 or origin_lon not between -180 and 180 or dest_lon not between -180 and 180
 or (time_valid and (actual_dep_utc is null or actual_arr_utc is null or actual_arr_utc<actual_dep_utc
 or scheduled_dep_utc is null or scheduled_arr_utc is null or scheduled_arr_utc<scheduled_dep_utc))
