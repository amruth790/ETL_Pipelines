# Conditional Branching in Airflow ETL

# Description:
#   DAG demonstrating Branching — conditionally executing ETL steps
#   depending on file availability and data quality checks.

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import sqlite3
import os

# -------------------------
# Configurations
# -------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True, parents=True)
DATASET = "conditional_customers"
CSV_PATH = DATA_DIR / f"{DATASET}.csv"

default_args = {
    "owner": "aravind",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# -------------------------
# Step 1: Extract Data (create sample CSV)
# -------------------------
def extract():
    df = pd.DataFrame({
        "id": range(1, 6),
        "name": ["Aravind", "Rahul", "Priya", "John", "Sara"],
        "city": ["Coventry", "London", "Manchester", "Leeds", "Bristol"]
    })
    df.to_csv(CSV_PATH, index=False)
    print(f" Extracted sample data to {CSV_PATH}")

# -------------------------
# Step 2: Check if file exists & valid
# -------------------------
def check_file_condition():
    if not CSV_PATH.exists():
        print(f" File missing: {CSV_PATH}")
        return "skip_etl"
    elif os.path.getsize(CSV_PATH) < 50:
        print(f" File too small, skipping ETL.")
        return "skip_etl"
    else:
        print(f"File valid, proceeding with ETL.")
        return "transform"

# -------------------------
# Step 3: Transform Data
# -------------------------
def transform():
    df = pd.read_csv(CSV_PATH)
    df["signup_date"] = pd.date_range("2023-01-01", periods=len(df))
    df.to_csv(DATA_DIR / f"{DATASET}_transformed.csv", index=False)
    print("Transformed data successfully.")


# Step 4: Load into SQLite

def load():
    conn = sqlite3.connect(DATA_DIR / "conditional_etl.db")
    df = pd.read_csv(DATA_DIR / f"{DATASET}_transformed.csv")
    df.to_sql("customers", conn, if_exists="replace", index=False)
    conn.close()
    print("Loaded data into SQLite database.")


# DAG Definition

with DAG(
    dag_id="conditional_branching_etl",
    description="ETL pipeline with conditional branching",
    default_args=default_args,
    start_date=datetime(2025, 10, 18),
    schedule_interval="@daily",
    catchup=False,
    tags=["week", "branching", "etl"],
) as dag:

    start = DummyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    branch = BranchPythonOperator(
        task_id="check_file_condition",
        python_callable=check_file_condition,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    skip = DummyOperator(task_id="skip_etl")

    end = DummyOperator(task_id="end")

