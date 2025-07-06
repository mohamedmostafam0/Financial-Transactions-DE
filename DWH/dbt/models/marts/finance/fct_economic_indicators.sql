-- models/marts/finance/fct_economic_indicators.sql

with stg_economic_indicators as (
    select
        indicator_id,
        indicator_name,
        value,
        date,
        country
    from {{ ref('stg_economic_indicators') }}
)

select
    indicator_id,
    indicator_name,
    value,
    date,
    country
from stg_economic_indicators
