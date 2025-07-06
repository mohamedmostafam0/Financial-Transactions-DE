-- models/marts/finance/dim_user.sql

with stg_transactions as (
    select
        user_id,
        -- In a real scenario, you would join with a user table to get more attributes
        current_timestamp as created_at
    from {{ ref('stg_transactions') }}
)

select
    user_id,
    created_at
from stg_transactions
group by user_id, created_at
