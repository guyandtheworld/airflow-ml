from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from utils.model.auto_classification import auto_class


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
    'auto_classification', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False, max_active_runs=1)


oil = PythonOperator(task_id='oil',
                     python_callable=auto_class,
                     op_kwargs={"scenario": "Oil"},
                     dag=dag)

sustainable = PythonOperator(task_id='sustainable',
                             python_callable=auto_class,
                             op_kwargs={"scenario": "Sustainable Finance"},
                             dag=dag)


crypto = PythonOperator(task_id='crypto',
                        python_callable=auto_class,
                        op_kwargs={"scenario": "Crypto"},
                        dag=dag)


recruiting = PythonOperator(task_id='recruiting',
                            python_callable=auto_class,
                            op_kwargs={"scenario": "Recruiting"},
                            dag=dag)


oil >> sustainable >> crypto >> recruiting
