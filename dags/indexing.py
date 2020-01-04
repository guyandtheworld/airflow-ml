import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/indexing_stages".format(path.parent))  # noqa

from index_article import index_articles
from index_entity import index_entities


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'indexing', default_args=default_args, schedule_interval=timedelta(days=1))

idx_entities = PythonOperator(task_id='index_entities',
                              python_callable=index_entities, dag=dag)

idx_articles = PythonOperator(task_id='index_articles',
                              python_callable=index_articles, dag=dag)

idx_entities >> idx_articles
