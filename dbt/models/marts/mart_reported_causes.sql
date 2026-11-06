-- Ported from the original analytical SQL.
SELECT 'Carrier' AS cause, sum(carrier_delay_min) AS reported_minutes,count(carrier_delay_min) AS nonnull_records FROM {{ ref('fact_flights') }}
UNION ALL SELECT 'Weather',sum(weather_delay_min),count(weather_delay_min) FROM {{ ref('fact_flights') }}
UNION ALL SELECT 'National aviation system',sum(nas_delay_min),count(nas_delay_min) FROM {{ ref('fact_flights') }}
UNION ALL SELECT 'Security',sum(security_delay_min),count(security_delay_min) FROM {{ ref('fact_flights') }}
UNION ALL SELECT 'Late-arriving aircraft',sum(late_aircraft_delay_min),count(late_aircraft_delay_min) FROM {{ ref('fact_flights') }}
