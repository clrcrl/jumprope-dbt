with pedometer_intervals as (
    select * from {{ ref('pedometer_intervals') }}
),

jump_sections as (
    select * from {{ ref('jump_sections') }}
),

break_sections as (
    select * from {{ ref('break_sections') }}
),

workout_streaks as (
    select
        workout_id,
        sum(total_seconds) as total_jumping_time,
        count(*) as n_streaks,
        median(total_steps) as median_jumps_per_streak,
        avg(total_steps) as average_jumps_per_streak,
        max(total_steps) as best_streak

    from jump_sections
    group by 1
),

workout_breaks as (
    select
        workout_id,
        sum(total_seconds) as total_break_time,
        count(*) as n_breaks

    from break_sections
    group by 1
),

workouts_aggregated as (
    select
        workout_id,
        min(interval_start_at) as start_at,
        max(interval_end_at) as end_at,
        sum(steps_this_interval) as total_steps,
        sum(seconds_this_interval) as total_seconds

    from pedometer_intervals
    group by 1
),

joined as (
    select
        *,
        1.0 * total_jumping_time / total_seconds as jumping_ratio

    from workouts_aggregated
    left join workout_streaks using (workout_id)
    left join workout_breaks using (workout_id)
)

select * from joined
