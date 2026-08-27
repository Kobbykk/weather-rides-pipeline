with source as (
    select * from {{ source('raw', 'raw_daily_weather') }}
)

select
    weather_date,
    temperature_2m_max      as temp_max_c,
    temperature_2m_min      as temp_min_c,
    precipitation_sum       as precip_mm,
    snowfall_sum            as snowfall_cm,
    wind_speed_10m_max      as wind_max_kmh,
    case
        when snowfall_sum   > 0   then 'snow'
        when precipitation_sum > 10 then 'heavy_rain'
        when precipitation_sum > 0  then 'light_rain'
        else 'dry'
    end                     as weather_condition,
    source_month
from source