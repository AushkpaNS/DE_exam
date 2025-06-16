from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from datetime import datetime
from datetime import timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'timeout': 600
}

with DAG(
    'pipeline_dag',
    description='ETL pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
) as dag:

    # Задача: extract_data.py
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python3 /home/airflow/etl/extract_data.py',
        dag = dag
    )

    # Задача: transform_data.py
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /home/airflow/etl/transform_data.py',
        dag = dag
    )

    # Задача: train_model.py
    train_model = BashOperator(
        task_id='train_model',
        bash_command='python3 /home/airflow/etl/train_model.py',
        dag = dag
    )

    # Задача: model_pred.py
    model_pred = BashOperator(
        task_id='model_pred',
        bash_command='python3 /home/airflow/etl/model_pred.py',
        dag = dag
    )

    # Задача: load_data.py
    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /home/airflow/etl/load_data.py',
        dag = dag,
    )

    # Установка последовательных зависимостей
    extract_data >> transform_data >> train_model >> model_pred >> load_data
