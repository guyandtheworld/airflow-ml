from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.model.oil_classification import oil_classification


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
    'oil_classification', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False, max_active_runs=1)


oil_classification = PythonOperator(task_id='classify',
                                    python_callable=oil_classification,
                                    dag=dag)

oil_classification
