{{ config(materialized='incremental',incremental_strategy='delete+insert',unique_key='flight_id',
          on_schema_change='fail',pre_hook='{{ delete_changed_months() }}',
          indexes=[{'columns':['flight_id'],'unique':true},{'columns':['month']},
                   {'columns':['tail_number','scheduled_dep_utc']}]) }}
-- Delete and replace changed months in one dbt model transaction. This includes
-- withdrawn rows and supports backfills older than the maximum loaded month.
select s.* from {{ ref('stg_flights') }} s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} f where f.month=s.month and f.version_id=s.version_id)
{% endif %}
