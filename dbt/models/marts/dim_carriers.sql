with ranked as (
 select carrier_id,carrier,flight_date,row_number() over(partition by carrier_id order by flight_date desc,flight_id) as rn
 from {{ ref('fact_flights') }}
)
select carrier_id,carrier,flight_date as last_observed_date from ranked where rn=1
