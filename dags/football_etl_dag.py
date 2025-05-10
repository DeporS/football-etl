from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from your_module import referees_etl, api_sports_etl, print_database

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG('football_etl_dag', default_args=default_args, schedule_interval=None) as dag:
    etl_referees = PythonOperator(
        task_id='referees_etl',
        python_callable=referees_etl
    )

    etl_api = PythonOperator(
        task_id='api_sports_etl',
        python_callable=api_sports_etl,
        op_args=[39, 2023]
    )

    show_db = PythonOperator(
        task_id='print_database',
        python_callable=print_database
    )

    etl_referees >> etl_api >> show_db
