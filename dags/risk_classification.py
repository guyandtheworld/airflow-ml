import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from model.risk_classification import load_model_utils, load_data, \
    predict_scores, log_metrics

from data.mongo_setup import global_init

global_init()


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
    schedule_interval=timedelta(hours=1),
    catchup=False)


load_model_utils = PythonOperator(task_id='load_model_utils',
                                  python_callable=load_model_utils,
                                  dag=dag)

load_data = PythonOperator(task_id='load_data',
                           python_callable=load_data,
                           dag=dag)

predict_scores = PythonOperator(task_id='predict_scores',
                                python_callable=predict_scores,
                                dag=dag)

log_metrics = PythonOperator(task_id='log_metrics',
                             python_callable=log_metrics,
                             dag=dag)


load_model_utils >> load_data >> predict_scores >> log_metrics
