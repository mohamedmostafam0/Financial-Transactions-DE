-- models/staging/stg_transactions.sql
with source as (
    select
        record_content:transaction_id::string as transaction_id,
        record_content:user_id::integer as user_id,
        record_content:amount::decimal(18, 2) as amount,
        record_content:currency::string as currency_code, -- Renamed
        record_content:merchant_id::string as merchant_id,
        record_content:merchant_name::string as merchant_name,
        record_content:merchant_category::string as merchant_category,
        record_content:card_type::string as card_type,
        record_content:timestamp::timestamp_tz as transaction_timestamp_utc,
        record_content:location_country::string as location_country_code, -- Renamed
        record_content:location_city::string as location_city,
        record_content:latitude::float as latitude,
        record_content:longitude::float as longitude,
        record_content:is_fraud::integer as is_fraud_flag -- Use flag suffix
        -- record_metadata:CreateTime::timestamp_tz as kafka_load_timestamp -- Optional metadata
    from {{ source('raw_data', 'raw_transactions') }}
    -- Optional: Add filter for incremental processing if needed later
    -- where record_metadata:CreateTime::timestamp_tz > (select max(kafka_load_timestamp) from {{ this }})
),

renamed_casted as (
    select
        transaction_id,
        user_id,
        amount,
        upper(currency_code) as currency_code, -- Standardize case
        merchant_id,
        merchant_name,
        merchant_category,
        card_type,
        transaction_timestamp_utc,
        date(transaction_timestamp_utc) as transaction_date_utc, -- Extract date
        upper(location_country_code) as location_country_code, -- Standardize case
        location_city,
        latitude,
        longitude,
        (is_fraud_flag = 1) as is_fraud -- Convert to boolean
        -- kafka_load_timestamp
    from source
    where transaction_id is not null -- Basic data quality filtering
      and user_id is not null
      and amount >= 0
      and transaction_timestamp_utc is not null
      and currency_code is not null
      and merchant_id is not null
)

select * from renamed_casted