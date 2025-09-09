{{ config(materialized='table') }}

with weather_data_cleansed as (
    select
        *,
        row_number() over (partition by city,time order by inserted_at) as rnk
    from {{ source('dev', 'raw_weather_data') }}
)

select 
    id,
    city,
    country,
    temperature,
    weather_description,
    wind_speed,
    wind_direction,
    humidity,
    visibility,
    time as weather_time_local,
    inserted_at + (utc_offset || ' hours')::interval as "inserted_time_local",
    utc_offset
from weather_data_cleansed
where rnk = 1
