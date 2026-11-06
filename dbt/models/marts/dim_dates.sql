select distinct flight_date,extract(year from flight_date)::integer as year,extract(month from flight_date)::integer as month,
 extract(isodow from flight_date)::integer as weekday,extract(week from flight_date)::integer as iso_week
from {{ ref('fact_flights') }}
