with source as (
    select
        record_content:transaction_id::string as transaction_id,
        record_content:user_id::integer as user_id,
        record_content:amount::decimal(18, 2) as amount,
        record_content:currency::string as currency,

        record_content:merchant_id::string as merchant_id,
        record_content:merchant_name::string as merchant_name,
        record_content:merchant_category::string as merchant_category,
        record_content:card_type::string as card_type,

        record_content:timestamp::timestamp_tz as transaction_timestamp_utc,
        record_content:location_country::string as location_country,
        record_content:location_city::string as location_city,
        record_content:latitude::float as latitude,
        record_content:longitude::float as longitude,

        record_metadata:CreateTime::timestamp_tz as kafka_load_timestamp
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
        -- Add basic transformations if needed
        convert_timezone('UTC', 'Africa/Cairo', transaction_timestamp_utc)::timestamp_ntz as transaction_timestamp_local, -- Example: Convert to local time
        date(transaction_timestamp_utc) as transaction_date_utc
        -- kafka_load_timestamp
    where 
        transaction_id is not null
        and user_id between 1 and 10000
        and amount >= 0.01 and amount <= 10000
        and currency rlike '^[A-Z]{3}$'
        and card_type in ('Visa', 'MasterCard', 'Amex', 'Discover', 'Other')
        and location_country rlike '^[A-Z]{2}$'
        and latitude between -90 and 90
        and longitude between -180 and 180
)

select * from renamed_casted