{{ config(
    materialized='incremental',
    unique_key='id'   
) }}

with weather_data_cleansed as (
    select
        *,
        row_number() over (partition by city, time order by inserted_at) as rnk
    from {{ source('dev', 'raw_weather_data') }}
)

select 
    id,
    city,
    country,
    latitude,
    longitude,
    temperature,
    weather_description,
    wind_speed,
    wind_direction,
    humidity,
    visibility,
    pressure,
    time as weather_time_local,
    inserted_at + (utc_offset || ' hours')::interval as inserted_time_local,
    utc_offset,
    run_id
from weather_data_cleansed
where rnk = 1

{% if is_incremental() %}
  -- Only insert records newer than what we already have
  and run_id > (select coalesce(max(run_id), '') from {{ this }})
{% endif %}