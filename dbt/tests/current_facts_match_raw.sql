(select flight_id,record_hash,version_id from {{ ref('fact_flights') }}
 except all select flight_id,record_hash,version_id from {{ source('raw','flights') }})
union all
(select flight_id,record_hash,version_id from {{ source('raw','flights') }}
 except all select flight_id,record_hash,version_id from {{ ref('fact_flights') }})
