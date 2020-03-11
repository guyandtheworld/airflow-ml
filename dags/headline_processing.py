from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.extraction.entity_extraction import entities_from_headlines
from utils.extraction.sentiment import sentiment_on_headlines


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
    schedule_interval=timedelta(hours=2),
    catchup=False)


entity_extraction = PythonOperator(task_id='entity_extraction',
                                   python_callable=entities_from_headlines,
                                   dag=dag)


sentiment_analysis = PythonOperator(task_id='sentiment_analysis',
                                    python_callable=sentiment_on_headlines,
                                    dag=dag)


entity_extraction >> sentiment_analysis
