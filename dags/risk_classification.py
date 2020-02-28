import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from model.risk_classification import risk_classification


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
    'risk_classification', default_args=default_args,
    schedule_interval=timedelta(hours=2),
    catchup=False)


risk_classification = PythonOperator(task_id='risk_classification',
                                     python_callable=risk_classification,
                                     dag=dag)

risk_classification
