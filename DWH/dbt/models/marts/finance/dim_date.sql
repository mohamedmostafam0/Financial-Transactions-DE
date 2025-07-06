-- models/marts/finance/dim_date.sql

with date_spine as (
  {{ dbt_utils.date_spine(
      start_date='2020-01-01',
      end_date='2026-01-01')
  }}
)

select
    date_day as date_id,
    date_day as full_date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    extract(dayofyear from date_day) as day_of_year,
    extract(quarter from date_day) as quarter,
    FORMAT_DATE('%B', date_day) as month_name,
    FORMAT_DATE('%A', date_day) as day_name
from date_spine
