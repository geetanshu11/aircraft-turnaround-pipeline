select f.*,
 count(*) filter(where scheduled_dep_utc is null) over(partition by tail_number,flight_date) as unorderable_same_day,
 (b.tail_number is not null or m.month is not null) as quarantine_barrier
from {{ ref('fact_flights') }} f
left join {{ source('raw','sequence_barriers') }} b using(month,tail_number,flight_date)
left join {{ source('raw','blocked_months') }} m using(month)
where f.tail_number is not null
