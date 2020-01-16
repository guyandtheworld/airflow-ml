import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from preprocessing.entity_extraction import extract_entities
from preprocessing.sentiment import sentiment_analysis


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
    'headline_preprocessing', default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False)


entity_extraction = PythonOperator(task_id='entity_extraction',
                                   python_callable=extract_entities,
                                   dag=dag,
                                   params={"dag": "title_analytics"})

sentiment_analysis = PythonOperator(task_id='sentiment_analysis',
                                    python_callable=sentiment_analysis,
                                    dag=dag,
                                    params={"dag": "title_analytics"})


entity_extraction >> sentiment_analysis
