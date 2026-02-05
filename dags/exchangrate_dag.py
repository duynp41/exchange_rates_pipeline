import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

API_URL = "https://api.exchangeratesapi.io/v1/latest"
DATA_DIR = "/opt/airflow/dags/data"


def fetch_exchange_rate(**context):
    execution_date = context["ds"]  # '2025-12-08'

    api_key = os.getenv("API_KEY")

    params = {
        "access_key": api_key,
        "base": "EUR"
    }

    response = requests.get(API_URL, params=params)
    data = response.json()

    df = pd.DataFrame([data["rates"]])
    df["date"] = data.get("date", execution_date)

    os.makedirs(DATA_DIR, exist_ok=True)

    file_path = f"{DATA_DIR}/exchange_rate/exchange_rate_{df['date'].iloc[0]}.csv"
    df.to_csv(file_path, index=False)

    return file_path

def create_postgres_table():
    hook = PostgresHook(postgres_conn_id="postgres_exchange")
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS exchange_rates (
            currency_code TEXT,
            rate DOUBLE PRECISION,
            date DATE,
            PRIMARY KEY (currency_code, date)
        );
    """
    hook.run(create_table_sql)
    print("Table exchange_rates created (or already exists).")

def upsert_exchange_rates(**context):
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="fetch_exchange_rate")
    
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    df = pd.read_csv(file_path)
    
    df_normalized = df.melt(
        id_vars=["date"],
        var_name="currency_code",
        value_name="rate"
    )
    
    df_normalized["date"] = pd.to_datetime(df_normalized["date"]).dt.date
    
    hook = PostgresHook(postgres_conn_id="postgres_exchange")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    upsert_sql = """
        INSERT INTO exchange_rates (currency_code, rate, date)
        VALUES (%s, %s, %s)
        ON CONFLICT (currency_code, date)
        DO UPDATE SET rate = EXCLUDED.rate;
    """
    
    rows = df_normalized[["currency_code", "rate", "date"]].to_records(index=False)
    cursor.executemany(upsert_sql, rows)
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print(f"Inserted/Updated {len(df_normalized)} exchange rate rows.")
    
def send_csv_to_telegram(**context):
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="fetch_exchange_rate")
    
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token:
        raise ValueError("Telegram token is not found.")
    elif not chat_id:
        raise ValueError("chat_id not found.")
    
    url=f"https://api.telegram.org/bot{token}/sendDocument"
    
    with open(file_path, "rb") as f:
        response = requests.post(
            url,
            data={"chat_id": chat_id},
            files={"document": f}
        )
    
    if response.status_code != 200:
        raise Exception(f"Telegram API error: {response.text}")
    
    print(f"CSV file sent succesfully to Telegram: {file_path}")
    
default_args = {
    "owner": "duy",
    "depend_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="exchange_rate_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule="0 1 * * *",
    catchup=True
) as dag:
    task_fetch = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate
    )
    
    task_create_table = PythonOperator(
        task_id="task_create_table",
        python_callable=create_postgres_table
    )
    
    task_upsert = PythonOperator(
        task_id="upsert_exchange_rates",
        python_callable=upsert_exchange_rates
    )
    
    task_send_telegram = PythonOperator(
        task_id="send_csv_to_telegram",
        python_callable=send_csv_to_telegram
    )
    
    task_fetch >> task_create_table >> task_upsert >> task_send_telegram
