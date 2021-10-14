with logging as (
    select * from {{ source('jumprope_firebase', 'logging') }}
),

renamed as (
    select
        sessionTimestamp as workout_id,
        timestamp_millis(sessionTimestamp) as start_at,
        steps,
        gyroscope,
        accelerometer
    from logging
)

select * from renamed
