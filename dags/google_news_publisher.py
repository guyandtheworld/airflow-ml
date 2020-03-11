from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.publisher.publish_to_source import publish_to_source


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
                                       python_callable=publish_to_source,
                                       op_kwargs={"source": "google_news",
                                                  "source_uuid": "1c74e10b-30fd-4052-9c00-eec0fc0ecdcf",
                                                  "timedelta": 1},
                                       dag=dag)

google_news_publisher
