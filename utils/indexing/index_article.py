import json
import logging
import os
import shutil

from datetime import datetime, timedelta

from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud.storage import Blob

from data.mongo_setup import global_init
from .utils import process_company_json, write_article, update_entity


logging.basicConfig(level=logging.INFO)


BUCKET_NAME = os.getenv("BUCKET_NAME", "alrtai-testing-bucket")
DESTINATION_FOLDER = "temp"
PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "index_articles")


def index_file(bucket, params: dict):
    processed_records = []
    logging.info("processing record: {}".format(params["source_file"]))

    processed_records = process_company_json(params, bucket)

    if len(processed_records) > 0:
        print("writing {} articles, {} into db".format(
            len(processed_records), params['id']))
        resp = write_article(processed_records)
    else:
        resp = {"status": "success",
                "data": "no articles to insert"}
    return resp


def verify_format(params: dict):
    keys = ["id", "history_processed",
            "last_tracked", "source_file"]

    for key in keys:
        if key not in params:
            return None

    params["id"] = int(params["id"])
    params["history_processed"] = json.loads(params["history_processed"])
    return params


def index_articles():
    """
    indexes articles by subscribing to news aggregator output

    * https://github.com/googleapis/google-cloud-java/issues/1661
    * https://stackoverflow.com/questions/48196679/gcp-pubsub-synchronous-pull-subscriber-in-python
    """
    # setup connection to database
    global_init()

    print("loading storage client")
    storage_client = storage.Client()

    if os.path.exists(DESTINATION_FOLDER):
        shutil.rmtree(DESTINATION_FOLDER)

    bucket = storage_client.bucket(BUCKET_NAME)
    os.mkdir(DESTINATION_FOLDER)

    client = pubsub_v1.SubscriberClient()

    subscription_path = client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    def callback(message):
        logging.info(
            "Received message {} of message ID {}\n".format(
                message, message.message_id
            )
        )

        params = {}
        if message.attributes:
            for key in message.attributes:
                value = message.attributes.get(key)
                params[key] = value

        params["source_file"] = message.data

        params = verify_format(params)

        if params:
            try:
                response = index_file(bucket, params)
                logging.info(response)
                message.ack()
                # os.rmdir(DESTINATION_FOLDER)
            except Exception as e:
                logging.info(
                    "message processing failed. up for retry. - " + str(e))
        else:
            logging.info("message format broken")


    streaming_pull_future = client.subscribe(
        subscription_path, callback=callback
    )
    logging.info("Listening for messages on {}..\n".format(subscription_path))

    try:
        streaming_pull_future.result()
    except:  # noqa
        streaming_pull_future.cancel()


if __name__ == "__main__":
    index_articles()
