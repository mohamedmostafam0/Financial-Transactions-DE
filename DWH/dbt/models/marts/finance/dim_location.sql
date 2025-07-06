-- models/marts/finance/dim_location.sql

with stg_transactions as (
    select
        location_country as country,
        location_city as city,
        latitude,
        longitude
    from {{ ref('stg_transactions') }}
)

select
    country,
    city,
    latitude,
    longitude
from stg_transactions
group by country, city, latitude, longitude
