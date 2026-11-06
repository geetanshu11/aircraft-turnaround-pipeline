select 'daily_total' as failed_check
where (select count(*) from {{ ref('fact_flights') }})<>(select coalesce(sum(scheduled_flights),0) from {{ ref('mart_daily_reliability') }})
union all
select 'monthly_departure_total'
where (select count(*) from {{ ref('fact_flights') }})<>(select coalesce(sum(scheduled_departures),0) from {{ ref('mart_monthly_airport_performance') }})
