from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.clustering.story_clustering import backfill


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 1),
    'end_date': datetime(2020, 8, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}


dag = DAG(
    'backfill_clustering', default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=True, max_active_runs=1)


clustering = PythonOperator(task_id='cluster',
                                    python_callable=backfill,
                                    dag=dag,
                                    provide_context=True)

clustering
