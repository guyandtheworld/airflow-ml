from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.extraction.entity_extraction import extract_entities


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}


dag = DAG(
    'entity_extraction', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False, max_active_runs=1)


entity_extraction = PythonOperator(task_id='extract_entities',
                                   python_callable=extract_entities,
                                   dag=dag)


entity_extraction
