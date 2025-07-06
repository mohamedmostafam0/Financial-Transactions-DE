-- models/staging/stg_economic_indicators.sql

with source as (
    select
        cast(json_extract_scalar(record_content, '$.indicator_id') as string) as indicator_id,
        cast(json_extract_scalar(record_content, '$.indicator_name') as string) as indicator_name,
        cast(json_extract_scalar(record_content, '$.value') as float64) as value,
        cast(json_extract_scalar(record_content, '$.date') as date) as date,
        cast(json_extract_scalar(record_content, '$.country') as string) as country
    from `sunlit-pixel-456910-j0`.`raw`.`raw_economic_indicators`
)

select
    indicator_id,
    indicator_name,
    value,
    date,
    country
from source