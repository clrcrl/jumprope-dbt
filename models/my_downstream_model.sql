select * from {{ ref('my_ephemeral_model')}}
limit 10