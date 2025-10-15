# Day 23: Airflow Introduction – Minimal ETL DAG

# DAG: simple_etl
#
# What it does:
# - Ensures a sample customers.csv exists next to this DAG
# - Extract: reads CSV with pandas
# - Transform: fills missing names, title-cases cities, parses dates, adds signup_year
# - Load: writes to a local SQLite DB (customers_db.sqlite) next to the DAG
#
# Note: Place this file under $AIRFLOW_HOME/dags/ (or your Airflow dags folder).

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import pandas as pd
import os

# Paths relative to this DAG file
DAG_DIR = Path(__file__).resolve().parent
DATA_DIR = DAG_DIR / "data"
CSV_PATH = DATA_DIR / "customers.csv"
SQLITE_PATH = DATA_DIR / "customers_db.sqlite"

def etl():
    # Ensure data folder exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # If no CSV yet, create a small sample
    if not CSV_PATH.exists():
        sample = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Aravind", "Rahul", "Priya", None, "John"],
            "city": ["Coventry", "London", "Manchester", "London", "London"],
            "signup_date": ["2023-01-01", "2023-02-15", "2023-03-10", "2023-01-25", "2023-04-05"]
        })
        sample.to_csv(CSV_PATH, index=False)

    # ---- Extract
    df = pd.read_csv(CSV_PATH)

    # ---- Transform (no inplace to avoid pandas chained-assignment warnings)
    df["name"] = df["name"].fillna("Unknown")
    df["city"] = df["city"].astype(str).str.title().str.strip()
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["signup_year"] = df["signup_date"].dt.year

    # ---- Load
    conn = sqlite3.connect(SQLITE_PATH)
    df.to_sql("customers", conn, if_exists="replace", index=False)
    conn.close()

    print(f"ETL complete. CSV: {CSV_PATH}  ->  DB: {SQLITE_PATH} (table=customers)")

default_args = {
    "owner": "aravind",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_etl",
    description="Day 23: Minimal ETL DAG (CSV -> Transform -> SQLite)",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),  # in the past so it can run immediately
    schedule_interval="@daily",
    catchup=False,
    tags=["week4", "etl", "intro"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl,
    )

    run_etl
