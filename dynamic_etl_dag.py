# Dynamic ETL DAG in Airflow

# Description:
#   Dynamic DAG that automatically generates ETL tasks for multiple CSV files.
#   Each file follows Extract → Transform → Load logic.

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import sqlite3


# Configurations

DATASETS = ["customers", "orders", "products"]
BASE_DIR = Path(__file__).resolve().parent / "data"

# Create folder if not exist
BASE_DIR.mkdir(exist_ok=True, parents=True)

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ETL Logic for Each Dataset

def extract(dataset_name):
    path = BASE_DIR / f"{dataset_name}.csv"
    df = pd.DataFrame({
        "id": range(1, 6),
        "value": [f"{dataset_name}_val_{i}" for i in range(1, 6)]
    })
    df.to_csv(path, index=False)
    print(f" Extracted {dataset_name}.csv")

def transform(dataset_name):
    path = BASE_DIR / f"{dataset_name}.csv"
    df = pd.read_csv(path)
    df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(BASE_DIR / f"{dataset_name}_transformed.csv", index=False)
    print(f" Transformed {dataset_name}_transformed.csv")

def load(dataset_name):
    db_path = BASE_DIR / "etl_dynamic.db"
    conn = sqlite3.connect(db_path)
    df = pd.read_csv(BASE_DIR / f"{dataset_name}_transformed.csv")
    df.to_sql(dataset_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f" Loaded {dataset_name} into {db_path}")


# DAG Definition

with DAG(
    dag_id="dynamic_etl_pipeline",
    description="Day 25: Dynamic ETL pipeline for multiple CSVs",
    default_args=default_args,
    start_date=datetime(2025, 10, 17),
    schedule_interval="@daily",
    catchup=False,
    tags=["week4", "etl", "dynamic"],
) as dag:

    for dataset in DATASETS:
        extract_task = PythonOperator(
            task_id=f"extract_{dataset}",
            python_callable=extract,
            op_args=[dataset]
        )

        transform_task = PythonOperator(
            task_id=f"transform_{dataset}",
            python_callable=transform,
            op_args=[dataset]
        )

        load_task = PythonOperator(
            task_id=f"load_{dataset}",
            python_callable=load,
            op_args=[dataset]
        )

#      extract_task >> transform_task >> load_task
