-- models/marts/finance/fct_transactions.sql

with stg_transactions as (
    select
        transaction_id,
        user_id,
        amount,
        currency,
        merchant_id,
        timestamp as transaction_timestamp_utc,
        location_country,
        location_city,
        latitude,
        longitude
    from {{ ref('stg_transactions') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_users as (
    select * from {{ ref('dim_users') }}
),

dim_merchants as (
    select * from {{ ref('dim_merchants') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
)

select
    t.transaction_id,
    d.date_id as date_key,
    u.user_id as user_key,
    m.merchant_id as merchant_key,
    l.country as location_key,
    t.amount,
    t.currency
from stg_transactions t
left join dim_date d on to_date(t.transaction_timestamp_utc) = d.full_date
left join dim_users u on t.user_id = u.user_id
left join dim_merchants m on t.merchant_id = m.merchant_id
left join dim_location l on t.location_country = l.country and t.location_city = l.city and t.latitude = l.latitude and t.longitude = l.longitude
