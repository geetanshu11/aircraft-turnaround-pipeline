{% macro delete_changed_months() %}
  {% if is_incremental() %}
    delete from {{ this }} f using {{ source('meta','checkpoints') }} c
    where f.month=c.month and f.version_id<>c.version_id
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
