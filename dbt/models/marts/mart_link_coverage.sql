-- Ported from the original analytical SQL.
SELECT link_status,count(*) AS candidate_records FROM {{ ref('int_aircraft_connections') }} GROUP BY link_status
