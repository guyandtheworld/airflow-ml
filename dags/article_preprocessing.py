import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

from article_preprocessing_stages.hashing import greet
from article_preprocessing_stages.text_extraction import respond


default_args = {
    'owner': 'alrtai',
    'start_date': dt.datetime(2019, 12, 22, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('article_preprocessing',
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         ) as dag:

    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')

    opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet)

    opr_sleep = BashOperator(task_id='sleep_me',
                             bash_command='sleep 5')

    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)

opr_hello >> opr_greet >> opr_sleep >> opr_respond
