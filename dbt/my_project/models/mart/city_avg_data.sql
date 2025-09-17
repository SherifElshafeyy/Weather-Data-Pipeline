-- depends_on: {{ ref('weather_data_cleansed') }}

{{ config(materialized='table') }}

with city_avg_data as (
    select
        *
    from {{ ref('weather_data_cleansed') }}
)

select 
    city,
    date(weather_time_local) as day,
    avg(temperature) as avg_temperature,
    avg(wind_speed) as avg_wind_speed,
    avg(humidity) as avg_humidity,
    avg(visibility) as avg_visibility,
    avg(pressure) as avg_pressure
from city_avg_data
group by city, date(weather_time_local)