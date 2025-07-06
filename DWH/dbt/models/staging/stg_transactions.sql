with source as (
    select
        cast(json_extract_scalar(record_content, '$.transaction_id') as string) as transaction_id,
        cast(json_extract_scalar(record_content, '$.user_id') as int64) as user_id,
        cast(json_extract_scalar(record_content, '$.amount') as numeric) as amount,
        cast(json_extract_scalar(record_content, '$.currency') as string) as currency,
        cast(json_extract_scalar(record_content, '$.merchant_id') as string) as merchant_id,
        cast(json_extract_scalar(record_content, '$.merchant_name') as string) as merchant_name,
        cast(json_extract_scalar(record_content, '$.merchant_category') as string) as merchant_category,
        cast(json_extract_scalar(record_content, '$.card_type') as string) as card_type,
        cast(json_extract_scalar(record_content, '$.timestamp') as timestamp) as transaction_timestamp_utc,
        cast(json_extract_scalar(record_content, '$.location_country') as string) as location_country,
        cast(json_extract_scalar(record_content, '$.location_city') as string) as location_city,
        cast(json_extract_scalar(record_content, '$.latitude') as float64) as latitude,
        cast(json_extract_scalar(record_content, '$.longitude') as float64) as longitude,
        cast(json_extract_scalar(record_metadata, '$.CreateTime') as timestamp) as kafka_load_timestamp
    from {{ source('raw_data', 'raw_transactions') }}
),
renamed as (
    select
        transaction_id,
        user_id,
        amount,
        currency,
        merchant_id,
        merchant_name,
        merchant_category,
        card_type,
        transaction_timestamp_utc as timestamp,
        location_country,
        location_city,
        latitude,
        longitude,
        datetime(transaction_timestamp_utc, 'Africa/Cairo') as transaction_timestamp_local,
        date(transaction_timestamp_utc) as transaction_date_utc
    from source
    where 
        transaction_id is not null
        and user_id between 1 and 10000
        and amount >= 0.01 and amount <= 10000
        and regexp_contains(currency, r'^[A-Z]{3})
        and card_type in ('Visa', 'MasterCard', 'Amex', 'Discover', 'Other')
        and regexp_contains(location_country, r'^[A-Z]{2})
        and latitude between -90 and 90
        and longitude between -180 and 180
)

select * from renamed