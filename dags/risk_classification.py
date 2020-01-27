import os
import sys

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from model.risk_classification import load_model, load_data, \
    preprocess_data, make_prediction, log_metrics


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


load_data = PythonOperator(task_id='load_data',
                           python_callable=load_data,
                           dag=dag)

load_model = PythonOperator(task_id='load_model',
                            python_callable=load_model,
                            dag=dag)

preprocess_data = PythonOperator(task_id='preprocess_data',
                                 python_callable=preprocess_data,
                                 dag=dag)

make_prediction = PythonOperator(task_id='make_prediction',
                                 python_callable=make_prediction,
                                 dag=dag)

log_metrics = PythonOperator(task_id='log_metrics',
                             python_callable=log_metrics,
                             dag=dag)


load_data >> preprocess_data >> make_prediction >> log_metrics
load_model >> make_prediction
