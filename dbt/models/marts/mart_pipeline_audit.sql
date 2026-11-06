{{ config(materialized='view') }}
select c.month,c.version_id,c.committed_at,v.source_sha256,v.reference_hash,v.received_at,
 v.raw_rows,v.accepted_rows,v.rejected_rows,v.duplicate_rows,v.manifest->>'source_kind' as source_kind,
 (v.manifest->>'observed_days')::integer as observed_days,
 v.manifest->>'date_start' as first_observed_date,v.manifest->>'date_end' as last_observed_date
from {{ source('meta','checkpoints') }} c join {{ source('meta','partition_versions') }} v using(version_id)
