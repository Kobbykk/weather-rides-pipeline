with source as (
    select * from {{ source('raw', 'raw_yellow_trips') }}
),

renamed as (
    select
        tpep_pickup_datetime            as pickup_at,
        tpep_dropoff_datetime           as dropoff_at,
        date(tpep_pickup_datetime)      as pickup_date,
        passenger_count,
        trip_distance,
        pulocationid                    as pickup_location_id,
        dolocationid                    as dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        source_month,
        extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60
                                        as trip_minutes
    from source
)

select * from renamed
where trip_minutes between 1 and 300
    and to_char(pickup_date, 'YYYY-MM') = source_month