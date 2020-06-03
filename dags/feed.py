import time
import logging
import os
import json

from google.cloud import pubsub_v1
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from utils.data.postgres_utils import connect


logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "feed_generation")
RESULT = False


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


def get_callback(api_future, data, ref):
    """
    Wrap message data in the context of the callback function.
    """

    def callback(api_future):
        global RESULT

        try:
            logging.info(
                "Published message {} now has message ID {}".format(
                    data, api_future.result()
                )
            )
            ref["num_messages"] += 1
            RESULT = True
        except Exception:
            logging.info(
                "A problem occurred when publishing {}: {}\n".format(
                    data, api_future.exception()
                )
            )
            raise

    return callback


def publish(message):
    """
    Publishes a message to a Pub/Sub topic.
    """
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    data = message.encode("utf-8")
    ref = dict({"num_messages": 0})

    api_future = client.publish(topic_path, data=data)

    api_future.add_done_callback(get_callback(api_future, data, ref))

    while api_future.running():
        time.sleep(0.5)
        logging.info("Published {} message(s).".format(ref["num_messages"]))

    return RESULT


dag = DAG(
    'feed_generation', default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    catchup=False, max_active_runs=1)


def fetch_scenarios():
    """
    Publish message to generate feed
    """

    query = """
            select uuid, name, mode from apis_scenario
            where status = 'active'
            """
    data = connect(query)

    for scenario in data:
        message = json.dumps(
            {"scenario": scenario[0], "mode": scenario[2]})

        logging.info("publishing {}".format(scenario[1]))
        publish(message)


generate = PythonOperator(task_id='generate',
                          python_callable=fetch_scenarios,
                          dag=dag)

generate
