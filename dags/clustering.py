from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.clustering.story_clustering import clustering


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
    'clustering', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False, max_active_runs=1)


clustering = PythonOperator(task_id='clustering',
                                    python_callable=clustering,
                                    dag=dag)

clustering
