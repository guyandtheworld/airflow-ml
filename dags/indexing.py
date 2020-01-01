import os
import sys

from pathlib import Path
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/indexing_stages".format(path.parent))  # noqa

from index_article import index_articles
from index_entity import index_entities


default_args = {
    'owner': 'alrtai',
    'start_date': dt.datetime(2019, 12, 30, 00, 00, 00),
    'concurrency': 1,
    'retries': 1
}

with DAG('indexing',
         default_args=default_args,
         schedule_interval=timedelta(minutes=10),
         catchup=False
         ) as dag:

    idx_entities = PythonOperator(task_id='index_entities',
                                  python_callable=index_entities)

    idx_articles = PythonOperator(task_id='index_articles',
                                  python_callable=index_articles)

idx_entities >> idx_articles
