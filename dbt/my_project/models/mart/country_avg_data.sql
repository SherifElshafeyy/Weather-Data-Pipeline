{{ config(materialized='table') }}

with country_avg_data as (
    select
        *
    from {{ ref('weather_data_cleansed') }}
)

select 
    country,
    date(weather_time_local) as day,
    avg(temperature) as avg_temperature,
    avg(wind_speed) as avg_wind_speed,
    avg(humidity) as avg_humidity,
    avg(visibility) as avg_visibility
from country_avg_data
group by country, date(weather_time_local)
