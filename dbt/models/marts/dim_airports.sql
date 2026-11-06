with observations as (
 select origin_id as airport_id,origin as airport,origin_city as city,origin_tz as timezone,
 origin_lat as latitude,origin_lon as longitude,flight_date,flight_id from {{ ref('fact_flights') }}
 union all
 select dest_id,dest,dest_city,dest_tz,dest_lat,dest_lon,flight_date,flight_id from {{ ref('fact_flights') }}
), ranked as (
 select *,row_number() over(partition by airport_id order by flight_date desc,flight_id,airport) as rn from observations
)
select airport_id,airport,city,timezone,latitude,longitude,flight_date as last_observed_date from ranked where rn=1
