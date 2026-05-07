from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Database connection
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# --- 1. DATA WAREHOUSE INITIALIZATION ---
def init_warehouse():
    """Generates synthetic data for users, marketing costs, and revenue."""
    NUM_USERS = 1000
    channels = ['Facebook Ads', 'TikTok Ads', 'Google Search', 'Organic']
    
    # User demographics
    df_users = pd.DataFrame({
        'user_id': range(1, NUM_USERS + 1),
        'acquisition_channel': np.random.choice(channels, NUM_USERS, p=[0.3, 0.4, 0.1, 0.2]),
        'signup_date': [datetime(2026, 1, 1) + timedelta(days=np.random.randint(0, 30)) for _ in range(NUM_USERS)]
    })
    df_users.to_sql('dim_users', con=engine, if_exists='replace', index=False)
    
    # Marketing spend per channel
    marketing_data = {
        'acquisition_channel': ['Facebook Ads', 'TikTok Ads', 'Google Search', 'Organic'],
        'total_spend_usd': [1200.00, 800.00, 300.00, 0.00]
    }
    pd.DataFrame(marketing_data).to_sql('dim_marketing_costs', con=engine, if_exists='replace', index=False)

    # Ad revenue simulation (Low initial LTV setting)
    ad_rev = []
    for uid in range(1, NUM_USERS + 1):
        for _ in range(np.random.randint(5, 25)):
            ad_rev.append({
                'event_id': np.random.randint(100000, 999999),
                'user_id': uid,
                'revenue_usd': np.random.uniform(0.005, 0.015),
                'date': datetime(2026, 1, np.random.randint(1, 28))
            })
    pd.DataFrame(ad_rev).to_sql('fact_ad_revenue', con=engine, if_exists='replace', index=False)
    
    # Subscription simulation (Low conversion)
    pd.DataFrame({'plan_id': [1, 2], 'price_usd': [9.99, 99.00]}).to_sql('dim_plans', con=engine, if_exists='replace', index=False)
    subs = [{'user_id': uid, 'plan_id': 1} for uid in range(1, 16)]
    pd.DataFrame(subs).to_sql('fact_subscriptions', con=engine, if_exists='replace', index=False)

# --- 2. ROI ANALYSIS & PERFORMANCE STATUS ---
def run_roi_status():
    """Calculates CAC, LTV, and Profitability status per channel."""
    query = """
    WITH channel_stats AS (SELECT acquisition_channel, COUNT(user_id) as user_count FROM dim_users GROUP BY acquisition_channel),
    revenue_stats AS (
        SELECT u.acquisition_channel, SUM(COALESCE(ad.rev, 0) + COALESCE(sub.rev, 0)) as total_revenue
        FROM dim_users u
        LEFT JOIN (SELECT user_id, SUM(revenue_usd) as rev FROM fact_ad_revenue GROUP BY user_id) ad ON u.user_id = ad.user_id
        LEFT JOIN (SELECT s.user_id, p.price_usd as rev FROM fact_subscriptions s JOIN dim_plans p ON s.plan_id = p.plan_id) sub ON u.user_id = sub.user_id
        GROUP BY u.acquisition_channel
    )
    SELECT m.acquisition_channel, m.total_spend_usd as marketing_spend, c.user_count,
    ROUND((m.total_spend_usd / NULLIF(c.user_count, 0))::numeric, 2) as CAC,
    ROUND((r.total_revenue / NULLIF(c.user_count, 0))::numeric, 2) as LTV,
    ROUND((r.total_revenue - m.total_spend_usd)::numeric, 2) as net_profit,
    CASE WHEN r.total_revenue > m.total_spend_usd THEN 'PROFITABLE' ELSE 'LOSS' END as status
    FROM dim_marketing_costs m JOIN channel_stats c ON m.acquisition_channel = c.acquisition_channel
    JOIN revenue_stats r ON m.acquisition_channel = r.acquisition_channel ORDER BY net_profit DESC
    """
    pd.read_sql(query, con=engine).to_sql('final_roi_report', con=engine, if_exists='replace', index=False)

# --- 3. PREDICTIVE MODEL (Path to Profitability) ---
def run_prediction():
    """Simulates cash flow and break-even points based on user engagement."""
    def get_rev(min_per_day):
        ads = min_per_day / 3
        m_rev = (ads * 30 * (12/1000)) + ((ads * 30 * 0.012) * 0.15)
        return m_rev

    time_slots = [2, 5, 10, 20, 40, 60, 90, 120]
    results = []
    for mins in time_slots:
        rev_u = get_rev(mins)
        users, cash, be_month = 0, 0, "Over 12 months"
        for m in range(1, 13):
            users += int(500/3.00)
            cash += (users * rev_u) - (500 + users * 0.02)
            if cash >= 0 and be_month == "Over 12 months": be_month = str(m)
        results.append({'mins_per_day': mins, 'rev_per_user': round(rev_u, 2), 'break_even_month': be_month, 'cash_after_1y': round(cash, 2)})
    pd.DataFrame(results).to_sql('yearly_business_forecast', con=engine, if_exists='replace', index=False)

with DAG('mobile_app_analytics_prod', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='init_data', python_callable=init_warehouse)
    t2 = PythonOperator(task_id='roi_analysis', python_callable=run_roi_status)
    t3 = PythonOperator(task_id='profit_forecast', python_callable=run_prediction)

    t1 >> t2 >> t3
