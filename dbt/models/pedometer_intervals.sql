with pedometer_measurements as (
    select * from {{ ref('pedometer_measurements') }}
),

pedometer_intervals as (
    select
        workout_id,

        workout_start_at,

        lag(pedometer_timestamp) over (
            partition by workout_id
            order by pedometer_timestamp
        ) as interval_start_at,

        pedometer_timestamp as interval_end_at,

        lag(seconds_since_start) over (
            partition by workout_id
            order by pedometer_timestamp
        ) as interval_starting_seconds,

        seconds_since_start as interval_ending_seconds,

        interval_ending_seconds - interval_starting_seconds as seconds_this_interval,

        lag(steps_since_start) over (
            partition by workout_id
            order by pedometer_timestamp
        ) as interval_starting_steps,

        steps_since_start as interval_ending_steps,

        interval_ending_steps - interval_starting_steps as steps_this_interval,

        steps_this_interval / seconds_this_interval as cadence


    from pedometer_measurements

    -- this filters out the first record
    qualify interval_start_at is not null
),

pedometer_intervals_categorized as (
    select
        *,

        -- chose an arbitrary number
        coalesce(cadence > 1.75, false) as is_jumping_section,

        lag(is_jumping_section) over (
            partition by workout_id
            order by interval_start_at
        ) as previous_is_jumping_section,

        coalesce(is_jumping_section != previous_is_jumping_section, true) as is_change_of_section_type

    from pedometer_intervals
),

pedometer_intervals_labelled as (

    select
        *,

        sum(is_change_of_section_type::integer) over (
            partition by workout_id, is_jumping_section
            order by interval_start_at
        ) as section_type_index,

        'workout-'|| workout_id || '-' || iff(is_jumping_section, 'jump', 'break') || '-' || section_type_index as section_id

    from pedometer_intervals_categorized

)

select * from pedometer_intervals_labelled
order by interval_start_at
