import os
import logging
import time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from google.cloud import pubsub_v1


logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "body_extraction")
RESULT = False


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 29),
    'email': ['adarsh@alrt.ai'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}


dag = DAG(
    'body_extraction', default_args=default_args,
    schedule_interval=timedelta(hours=2),
    catchup=False)


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


def publish():
    """
    Publishes a message to a Pub/Sub topic.
    """
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    data = "{}".format("body_extraction trigger").encode("utf-8")
    ref = dict({"num_messages": 0})

    api_future = client.publish(topic_path, data=data)

    api_future.add_done_callback(get_callback(api_future, data, ref))

    while api_future.running():
        time.sleep(0.5)
        logging.info("Published {} message(s).".format(ref["num_messages"]))

    return RESULT


publish_task = PythonOperator(task_id='publish',
                              python_callable=publish,
                              dag=dag)

publish_task
