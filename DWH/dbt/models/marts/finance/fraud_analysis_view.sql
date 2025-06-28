-- Finance - Fraud Analysis View
{{
    config(
        materialized='view',
        labels={'type': 'view', 'category': 'finance', 'purpose': 'analysis'}
    )
}}

WITH base_transactions AS (
    SELECT
        t.transaction_id,
        t.user_id,
        t.merchant_id,
        t.amount,
        t.currency_code,
        t.transaction_timestamp_utc,
        t.transaction_date_utc,
        t.is_fraud,
        t.card_type,
        t.location_country_code,
        t.location_city,
        t.latitude,
        t.longitude,
        
        -- User features
        u.current_age,
        u.credit_score,
        u.risk_level as user_risk_level,
        u.risk_score as user_risk_score,
        
        -- Merchant features
        m.risk_level as merchant_risk_level,
        m.risk_score as merchant_risk_score,
        
        -- Economic features
        e.unemployment_rate,
        e.unemployment_change_rate
    
    FROM {{ ref('fct_transactions') }} t
    LEFT JOIN {{ ref('dim_users') }} u ON t.user_id = u.user_id
    LEFT JOIN {{ ref('dim_merchants') }} m ON t.merchant_id = m.merchant_id
    LEFT JOIN {{ ref('fct_economic_indicators') }} e ON 
        DATE(t.transaction_timestamp_utc) = DATE(e.date) AND
        e.indicator_name = 'unemployment_rate'
)

SELECT
    -- Transaction features
    transaction_id,
    user_id,
    merchant_id,
    amount,
    currency_code,
    transaction_timestamp_utc,
    transaction_date_utc,
    is_fraud,
    
    -- User features
    current_age,
    credit_score,
    user_risk_level,
    user_risk_score,
    
    -- Merchant features
    merchant_risk_level,
    merchant_risk_score,
    
    -- Economic features
    unemployment_rate,
    unemployment_change_rate,
    
    -- Location features
    location_country_code,
    location_city,
    latitude,
    longitude,
    
    -- Card features
    card_type,
    
    -- Time features
    EXTRACT(HOUR FROM transaction_timestamp_utc) as transaction_hour,
    EXTRACT(DAYOFWEEK FROM transaction_timestamp_utc) as transaction_day_of_week

FROM base_transactions
