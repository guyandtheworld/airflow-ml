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


def publish(params):
    """
    Publishes a message to a Pub/Sub topic.
    """
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    name = "{}-{}-{}".format(params["entity_id"], params["entity_name"], params["date_from"])

    data = "{}".format(name).encode("utf-8")
    ref = dict({"num_messages": 0})

    params = {
        "entity_id": str(params["entity_id"]),
        "entity_name": params["entity_name"],
        "common_names": json.dumps(params["common_names"]),
        "scenario_id": str(params["scenario_id"]),
        "source": json.dumps(params["source"]),
        "date_from": params["date_from"],
        "date_to": params["date_to"],
        "storage_bucket": params["storage_bucket"],
        "history_processed": json.dumps(params["history_processed"])
    }

    api_future = client.publish(topic_path, data=data, **params)

    api_future.add_done_callback(get_callback(api_future, data, ref))

    while api_future.running():
        time.sleep(0.5)
        logging.info("Published {} message(s).".format(ref["num_messages"]))

    return RESULT
