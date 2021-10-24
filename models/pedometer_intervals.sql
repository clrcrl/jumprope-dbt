with logging as (
    select * from {{ source('jumprope', 'logging') }}
),

pedometer_measurements as (
    select
        -- TODO: fix this later
        1 as workout_id,

        loggingtime_txt as logging_time,

        first_value(logging_time) over (
            partition by workout_id
            order by logging_time
        ) as workout_start_at,

        datediff(
            'milliseconds',
            workout_start_at,
            logging_time
        ) / 1000.0 as seconds_since_start,

        pedometerNumberofSteps_N as pedometer_number_of_steps_on_watch,

        first_value(pedometer_number_of_steps_on_watch) over (
            partition by workout_id
            order by logging_time
        ) as initial_pedometer_value,

        pedometer_number_of_steps_on_watch - initial_pedometer_value as steps_since_start

    from logging

    where pedometer_number_of_steps_on_watch is not null
),

pedometer_intervals as (
    select
        workout_id,

        workout_start_at,

        lag(logging_time) over (
            partition by workout_id
            order by logging_time
        ) as interval_start_at,

        logging_time as interval_end_at,

        lag(seconds_since_start) over (
            partition by workout_id
            order by logging_time
        ) as interval_starting_seconds,

        seconds_since_start as interval_ending_seconds,

        interval_ending_seconds - interval_starting_seconds as seconds_this_interval,

        lag(steps_since_start) over (
            partition by workout_id
            order by logging_time
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
