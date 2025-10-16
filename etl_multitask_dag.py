# Airflow ETL DAG (Multi-Task Pipeline)

# Description:
#   Modular ETL pipeline using three PythonOperator tasks:
#   1. Extract: Read data from CSV
#   2. Transform: Clean and enrich data
#   3. Load: Store processed data into SQLite

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import sqlite3
import os

# Paths
DAG_DIR = Path(__file__).resolve().parent
DATA_DIR = DAG_DIR / "data"
CSV_PATH = DATA_DIR / "customers.csv"
TRANSFORMED_PATH = DATA_DIR / "customers_transformed.csv"
SQLITE_PATH = DATA_DIR / "customers_db.sqlite"

# -------------------------
# Define ETL Functions
# -------------------------
def extract():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    sample = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Aravind", "Rahul", "Priya", None, "John"],
        "city": ["Coventry", "London", "Manchester", "London", "London"],
        "signup_date": ["2023-01-01", "2023-02-15", "2023-03-10", "2023-01-25", "2023-04-05"]
    })
    sample.to_csv(CSV_PATH, index=False)
    print(f" Extracted raw data -> {CSV_PATH}")

def transform():
    df = pd.read_csv(CSV_PATH)
    df["name"] = df["name"].fillna("Unknown")
    df["city"] = df["city"].astype(str).str.title().str.strip()
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["signup_year"] = df["signup_date"].dt.year
    df.to_csv(TRANSFORMED_PATH, index=False)
    print(f" Transformed data saved -> {TRANSFORMED_PATH}")

def load():
    df = pd.read_csv(TRANSFORMED_PATH)
    conn = sqlite3.connect(SQLITE_PATH)
    df.to_sql("customers", conn, if_exists="replace", index=False)
    conn.close()
    print(f" Loaded data into SQLite -> {SQLITE_PATH}")

# -------------------------
# Airflow DAG Definition
# -------------------------
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="multitask_etl_pipeline",
    description=" Multi-Task ETL DAG (Extract → Transform → Load)",
    default_args=default_args,
    start_date=datetime(2025, 10, 16),
    schedule_interval="@daily",
    catchup=False,
    tags=["week4", "etl", "multitask"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    # Define dependencies
    extract_task >> transform_task >> load_task
