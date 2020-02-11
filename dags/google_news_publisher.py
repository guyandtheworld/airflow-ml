from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


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


def publish_google_news():
    pass


google_news_publisher = PythonOperator(task_id='load_model_utils',
                                       python_callable=publish_google_news,
                                       dag=dag)

google_news_publisher
