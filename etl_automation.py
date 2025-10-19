from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

default_args = {
    'owner': 'aravind',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def extract():
    df = pd.read_csv('/opt/airflow/dags/data/customers.csv')
    df.to_csv('/opt/airflow/dags/data/extracted.csv', index=False)

def transform():
    df = pd.read_csv('/opt/airflow/dags/data/extracted.csv')
    df['FullName'] = df['FirstName'] + ' ' + df['LastName']
    df.to_csv('/opt/airflow/dags/data/transformed.csv', index=False)

def load():
    df = pd.read_csv('/opt/airflow/dags/data/transformed.csv')
    conn = sqlite3.connect('/opt/airflow/dags/data/customers_db.sqlite')
    df.to_sql('customers', conn, if_exists='replace', index=False)
    conn.close()

with DAG('automated_etl',
         default_args=default_args,
         description='ETL Pipeline Automation using Airflow',
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_task = PythonOperator(task_id='extract_data', python_callable=extract)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform)
    load_task = PythonOperator(task_id='load_data', python_callable=load)


