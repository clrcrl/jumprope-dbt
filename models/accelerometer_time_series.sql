with workouts as (
    select * from {{ ref('workouts') }}
),

unnested as (
    select
        workouts.workout_id,
        workouts.start_at,
        timestamp_millis(a.timestamp) as timestamp,
        a.timestamp as timestamp_unix_millis,
        a.x,
        a.y,
        a.z,

    from workouts
    left join unnest(accelerometer) as a
)

select * from unnested
