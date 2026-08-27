with trips as (
    select
        pickup_date,
        count(*)                as trip_count,
        sum(total_amount)       as total_revenue,
        avg(total_amount)       as avg_fare,
        avg(tip_amount)         as avg_tip,
        avg(trip_distance)      as avg_distance_mi,
        avg(trip_minutes)       as avg_duration_min,
        sum(case when tip_amount > 0 then 1 else 0 end)::numeric
            / count(*)          as tipped_share
    from {{ ref('stg_trips') }}
    group by 1
),

weather as (
    select * from {{ ref('stg_weather') }}
)

select
    w.weather_date,
    w.weather_condition,
    round(w.temp_max_c::numeric, 1)     as temp_max_c,
    round(w.precip_mm::numeric, 2)      as precip_mm,
    t.trip_count,
    round(t.total_revenue::numeric, 2)  as total_revenue,
    round(t.avg_fare::numeric, 2)       as avg_fare,
    round(t.avg_tip::numeric, 2)        as avg_tip,
    round(t.avg_distance_mi::numeric, 2) as avg_distance_mi,
    round(t.avg_duration_min::numeric, 1) as avg_duration_min,
    round(t.tipped_share::numeric, 3)   as tipped_share
from weather w
left join trips t on t.pickup_date = w.weather_date