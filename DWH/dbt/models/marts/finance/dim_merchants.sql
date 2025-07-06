-- models/marts/finance/dim_merchant.sql

with stg_transactions as (
    select
        merchant_id,
        merchant_name,
        merchant_category
    from {{ ref('stg_transactions') }}
)

select
    merchant_id,
    merchant_name,
    merchant_category
from stg_transactions
group by merchant_id, merchant_name, merchant_category
