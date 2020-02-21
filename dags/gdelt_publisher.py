import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from publisher.publish_to_source import publish_to_source


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
    'gdelt_publisher', default_args=default_args,
    schedule_interval=timedelta(hours=9),
    catchup=False)


gdelt_publisher = PythonOperator(task_id='publish_to_gdelt_news_scraper',
                                 python_callable=publish_to_source,
                                 op_kwargs={"source": "gdelt",
                                 "source_uuid": "93d7f1d2-cb5a-4372-a255-50114765cd03",
                                 "timedelta": 9},
                                 dag=dag)

gdelt_publisher
