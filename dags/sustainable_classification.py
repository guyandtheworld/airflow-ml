from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.model.sustainable_classification import sustainable


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}


dag = DAG(
    'susfin_classification', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False, max_active_runs=1)


sustainable = PythonOperator(task_id='classify',
                             python_callable=sustainable,
                             dag=dag)

sustainable
