import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from extraction.entity_extraction import entities_from_body
from extraction.sentiment import sentiment_from_body
from extraction.body_extraction import extract_body


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
    'body_processing', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False)


body_extraction = PythonOperator(task_id='body_extraction',
                                 python_callable=extract_body,
                                 dag=dag)


entity_extraction = PythonOperator(task_id='entity_extraction',
                                   python_callable=entities_from_body,
                                   dag=dag)


sentiment_analysis = PythonOperator(task_id='sentiment_analysis',
                                    python_callable=sentiment_from_body,
                                    dag=dag)


body_extraction >> entity_extraction >> sentiment_analysis
