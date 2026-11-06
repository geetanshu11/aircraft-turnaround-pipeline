select c.month from {{ source('meta','checkpoints') }} c
join {{ source('meta','partition_versions') }} v using(version_id)
left join (select month,count(*) as n from {{ ref('fact_flights') }} group by month) f on f.month=c.month
where v.accepted_rows<>coalesce(f.n,0)
