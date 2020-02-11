import json
import logging
import os
import time

from google.cloud import pubsub_v1


logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "scraping")
RESULT = False


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


def publish(**kwargs):
    """
    Publishes a message to a Pub/Sub topic.
    """
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    data = "{}".format(kwargs["path"]).encode("utf-8")
    ref = dict({"num_messages": 0})

    params = {
        "id": str(kwargs["entity_id"]),
        "company_name": kwargs["entity_id"],
        "common_names": json.dumps(kwargs["common_names"]),
        "source": json.dumps(kwargs["source"]),
        "date_from": kwargs["date_from"],
        "date_to": kwargs["date_to"],
        "storage_bucket": kwargs["bucket"]
    }

    api_future = client.publish(topic_path, data=data, **params)

    api_future.add_done_callback(get_callback(api_future, data, ref))

    while api_future.running():
        time.sleep(0.5)
        logging.info("Published {} message(s).".format(ref["num_messages"]))

    return RESULT
