with pedometer_intervals as (
    select * from {{ ref('pedometer_intervals') }}
),

sections as (
    select
        workout_id,
        section_id,
        is_jumping_section,

        -- do we need all of this?
        min(interval_start_at) as start_at,
        max(interval_end_at) as end_at,
        sum(seconds_this_interval) as total_seconds,
        sum(steps_this_interval) as total_steps,
        total_steps / total_seconds as cadence_this_section

    from pedometer_intervals

    -- something feels off here. 
    group by 1, 2, 3
)

select * from sections
