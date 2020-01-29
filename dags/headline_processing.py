import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from extraction.entity_extraction import extract_entities_from_headlines
from extraction.sentiment import sentiment_analysis_on_headlines
from data.mongo_setup import global_init

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
    'headline_processing', default_args=default_args,
    schedule_interval=timedelta(hours=12),
    catchup=False)


entity_extraction = PythonOperator(task_id='entity_extraction',
                                   python_callable=extract_entities_from_headlines,
                                   dag=dag)


sentiment_analysis = PythonOperator(task_id='sentiment_analysis',
                                    python_callable=sentiment_analysis_on_headlines,
                                    dag=dag)


entity_extraction >> sentiment_analysis
