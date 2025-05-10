from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from database_manager import create_tables
from datetime import datetime

def initialize_database():
    create_tables() 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'your_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=None,
)

initialize_db_task = PythonOperator(
    task_id='initialize_database',
    python_callable=initialize_database,
    dag=dag,
)
