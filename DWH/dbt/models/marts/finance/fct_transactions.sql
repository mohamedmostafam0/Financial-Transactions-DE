-- dbt/models/marts/finance/fct_transactions.sql
-- Example fact table joining staged transactions with other dimensions (if they existed)

with transactions as (
    select * from {{ ref('stg_transactions') }}
)
-- Example: Add joins here if you had dimension tables like dim_users, dim_dates, dim_merchants
-- left join {{ ref('dim_users') }} users on transactions.user_id = users.user_id
-- left join {{ ref('dim_dates') }} dates on transactions.transaction_date_utc = dates.date_day

select
    -- Surrogate key (optional but good practice)
    -- {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as transaction_pk,

    -- Dimensions (FKs to dimension tables, or degenerate dimensions)
    transaction_id, -- Degenerate dimension
    user_id, -- Foreign key to dim_users
    -- users.user_sk, -- Example: Use surrogate key from dim_users
    -- dates.date_sk, -- Example: Use surrogate key from dim_dates

    -- Measures / Facts
    amount,
    is_fraud,

    -- Timestamps
    transaction_timestamp_utc,
    transaction_timestamp_local,
    transaction_date_utc

from transactions
-- Add joins here
-- ...