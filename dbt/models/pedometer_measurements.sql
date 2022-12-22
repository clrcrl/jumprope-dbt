with logging as (
    select * from {{ source('jumprope', 'logging') }}
),

-- need to filter out measurements that don't have pedometer data, and dedupe some of the data
filtered as (
    select * from logging

    where pedometerNumberofSteps_N is not null
        and pedometerenddate_txt is not null

    qualify row_number() over (
        partition by filename, pedometerenddate_txt
        order by file_row_number
    ) = 1
),

pedometer_measurements as (
    select
        md5(pedometerenddate_txt) as measurement_id,
        
        md5(filename) as workout_id,

        pedometerenddate_txt as pedometer_timestamp,

        first_value(pedometer_timestamp) over (
            partition by workout_id
            order by pedometer_timestamp
        ) as workout_start_at,

        datediff(
            'milliseconds',
            workout_start_at,
            pedometer_timestamp
        ) / 1000.0 as seconds_since_start,

        pedometerNumberofSteps_N as pedometer_number_of_steps_on_watch,

        first_value(pedometer_number_of_steps_on_watch) over (
            partition by workout_id
            order by pedometer_timestamp
        ) as initial_pedometer_value,

        pedometer_number_of_steps_on_watch - initial_pedometer_value as steps_since_start

    from filtered

)

select * from pedometer_measurements
