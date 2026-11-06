select month,airport_id from {{ ref('mart_monthly_airport_performance') }} group by month,airport_id having count(*)<>1
