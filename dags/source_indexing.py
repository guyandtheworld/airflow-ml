import os
import sys
import logging
import uuid

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from urllib.parse import urlparse

path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/utils".format(path.parent))  # noqa

from data.postgres_utils import connect, insert_values

logging.basicConfig(level=logging.INFO)


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
    'source_indexing', default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False)


def index_source():
    """
    * fetch values
    * get names
    * insert the sets that are not in the db with uuid
    """

    query = """
    select as2.domain from
    (select distinct "domain" from apis_story) as2
    left join
    apis_source src
    on as2."domain" = src."name"
    where src."name" is null
    """

    new_sources = connect(query)
    values = []
    logging.info("{} new domains".format(len(new_sources)))
    for source in new_sources:
        values.append(((str(uuid.uuid4()), source[0])))

    insert_query = """
                    INSERT INTO public.apis_source
                    (uuid, "name")
                    VALUES(%s, %s);
                   """

    insert_values(insert_query, values)


index_sources = PythonOperator(task_id='index_source',
                               python_callable=index_source,
                               dag=dag)

index_sources