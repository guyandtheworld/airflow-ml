import os
import sys

import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

from indexing_stages.index_article import index_articles
from indexing_stages.index_entity import index_entities

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

default_args = {
    'owner': 'alrtai',
    'start_date': dt.datetime(2019, 12, 30, 00, 00, 00),
    'concurrency': 1,
    'retries': 1
}

with DAG('indexing',
         default_args=default_args,
         schedule_interval=timedelta(hours=8),
         catchup=False
         ) as dag:

    idx_entities = PythonOperator(task_id='index_entities',
                                  python_callable=index_entities)

    idx_articles = PythonOperator(task_id='index_articles',
                                  python_callable=index_articles)

idx_entities >> idx_articles
