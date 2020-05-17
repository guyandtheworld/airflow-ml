from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.sentiment.analysis import (sentiment_on_headlines,
                                      sentiment_from_body)


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
    'sentiment_extraction', default_args=default_args,
    schedule_interval=timedelta(hours=2),
    catchup=False, max_active_runs=1)


headline_sentiment = PythonOperator(task_id='sentiment_title',
                                    python_callable=sentiment_on_headlines,
                                    dag=dag)


body_sentiment = PythonOperator(task_id='sentiment_body',
                                python_callable=sentiment_from_body,
                                dag=dag)


headline_sentiment >> body_sentiment
