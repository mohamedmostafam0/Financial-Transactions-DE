-- Finance - ML Features View
{{
    config(
        materialized='view',
        labels={'type': 'view', 'category': 'finance', 'purpose': 'ml'}
    )
}}

WITH base_features AS (
    SELECT
        -- Transaction features
        transaction_id,
        amount,
        transaction_hour,
        transaction_day_of_week,
        
        -- User features
        current_age,
        credit_score,
        user_risk_score,
        user_risk_level,
        
        -- Merchant features
        merchant_risk_score,
        merchant_risk_level,
        
        -- Economic features
        unemployment_rate,
        unemployment_change_rate,
        
        -- Location features
        latitude,
        longitude,
        
        -- Card features
        CASE
            WHEN card_type = 'chip' THEN 1
            ELSE 0
        END as use_chip,
        
        -- Pattern features
        COUNT(*) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_timestamp_utc 
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) as transaction_count_last_24h,
        
        AVG(amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_timestamp_utc 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as avg_amount_last_7d
    
    FROM {{ ref('fraud_analysis_view') }}
)

SELECT
    -- Features
    transaction_id,
    amount,
    transaction_hour,
    transaction_day_of_week,
    current_age,
    credit_score,
    user_risk_score,
    user_risk_level,
    merchant_risk_score,
    merchant_risk_level,
    unemployment_rate,
    unemployment_change_rate,
    latitude,
    longitude,
    use_chip,
    transaction_count_last_24h,
    avg_amount_last_7d,
    
    -- Label
    is_fraud

FROM base_features
