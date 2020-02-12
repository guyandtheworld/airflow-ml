import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from publisher.google_news import publish_google_news


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
    'google_news_publisher', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False)


google_news_publisher = PythonOperator(task_id='publish_to_google_news_scraper',
                                       python_callable=publish_google_news,
                                       dag=dag)

google_news_publisher
