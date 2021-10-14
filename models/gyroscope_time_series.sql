with workouts as (
    select * from {{ ref('workouts') }}
),

unnested as (
    select
        workouts.workout_id,
        workouts.start_at,
        timestamp_millis(g.timestamp) as timestamp,
        g.timestamp as timestamp_unix_millis,
        g.x,
        g.y,
        g.z,

    from workouts
    left join unnest(gyroscope) as g
)

select * from unnested
