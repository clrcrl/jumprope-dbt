with break_intervals as (
    select * from {{ ref('pedometer_intervals') }}
    where is_jumping_section = false
),

aggregated_breaks as (
    select
        workout_id,
        section_id,

        min(interval_start_at) as started_at,
        max(interval_end_at) as ended_at,
        sum(seconds_this_interval) as total_seconds,
        sum(steps_this_interval) as total_steps

    from break_intervals
    group by 1, 2
)

select * from aggregated_breaks
