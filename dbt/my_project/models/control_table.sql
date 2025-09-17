{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with control_table as (
    select 
        id,
        run_id,
        'pending' as status   -- default value for new records
    from {{ source('dev', 'raw_weather_data') }}
)

select * 
from control_table
where run_id is not null
{% if is_incremental() %}
  and id > (select max(id) from {{ this }})
{% endif %}
