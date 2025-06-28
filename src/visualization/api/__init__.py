from flask import Flask, jsonify
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd

app = Flask(__name__)
client = bigquery.Client()

@app.route('/api/dashboard/transactions')
def get_transaction_dashboard():
    """Get transaction dashboard metrics"""
    query = """
    WITH daily_transactions AS (
        SELECT
            DATE(transaction_date) as date,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            COUNT(DISTINCT client_id) as unique_users
        FROM `your-project.your-dataset.fact_transactions`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY date
    )
    SELECT
        date,
        transaction_count,
        total_amount,
        avg_amount,
        unique_users,
        ROUND((transaction_count - LAG(transaction_count) OVER (ORDER BY date)) * 100.0 / LAG(transaction_count) OVER (ORDER BY date), 2) as daily_change
    FROM daily_transactions
    ORDER BY date DESC
    LIMIT 30
    """
    
    df = client.query(query).to_dataframe()
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/dashboard/fraud')
def get_fraud_dashboard():
    """Get fraud detection dashboard metrics"""
    query = """
    WITH fraud_metrics AS (
        SELECT
            DATE(transaction_date) as date,
            COUNT(*) as total_transactions,
            COUNT(CASE WHEN is_fraud = TRUE THEN 1 END) as fraud_count,
            ROUND(COUNT(CASE WHEN is_fraud = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as fraud_rate,
            AVG(CASE WHEN is_fraud = TRUE THEN amount END) as avg_fraud_amount
        FROM `your-project.your-dataset.fraud_analysis_view`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY date
    )
    SELECT
        date,
        total_transactions,
        fraud_count,
        fraud_rate,
        avg_fraud_amount,
        ROUND((fraud_rate - LAG(fraud_rate) OVER (ORDER BY date)) * 100.0 / LAG(fraud_rate) OVER (ORDER BY date), 2) as daily_change
    FROM fraud_metrics
    ORDER BY date DESC
    LIMIT 7
    """
    
    df = client.query(query).to_dataframe()
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/dashboard/economic')
def get_economic_dashboard():
    """Get economic indicators dashboard"""
    query = """
    SELECT
        date,
        indicator_name,
        value,
        change_rate * 100 as percentage_change,
        LAG(value) OVER (PARTITION BY indicator_name ORDER BY date) as previous_value
    FROM `your-project.your-dataset.fact_economic_indicators`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ORDER BY indicator_name, date
    """
    
    df = client.query(query).to_dataframe()
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
