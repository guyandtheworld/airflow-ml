import json
import os

from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.storage import Blob

from data.entity import EntityIndex
from data.mongo_setup import global_init
from utils import process_company_json, write_article

# should store sources in the database
SOURCES = ["gdelt", "google_news"]
BUCKET_NAME = "alrt-ai-ps"
DESTINATION_FOLDER = "temp"


def process_new_entity_data(new_entities):
    """
    it processes the history if the entity is not tracked.
    fetches all files within the corresponding dir - process
    it and change last tracked to the latest date_to.

    # to do
    we need to update entity_search_name, ticker, public
    and aliases once we fetch first set of data
    """
    print("loading storage client")
    storage_client = storage.Client()

    records = []

    print("fetching new objects")
    for obj in new_entities:
        uid = obj.entity_id
        name = obj.entity_legal_name

        for source in SOURCES:
            prefix = "{}-{}/{}".format(uid, name, source)
            blobs = storage_client.list_blobs(
                BUCKET_NAME, prefix=prefix)

            for blob in blobs:
                record = {}
                record["file"] = blob.name
                record["id"] = uid
                record["name"] = name
                record["source"] = source
                record["entity_object"] = obj
                records.append(record)

    bucket = storage_client.bucket(BUCKET_NAME)
    os.mkdir(DESTINATION_FOLDER)

    all_records = []
    for record in records:
        print("processing record: {}".format(record["file"]))
        metadata = {
            "source_file": record["file"],
            "entity_object": record["entity_object"]
        }
        processed_records = process_company_json(record, bucket, metadata)
        all_records.extend(processed_records)

    if len(all_records) > 0:
        print("writing {} articles into db".format(len(all_records)))
        resp = write_article(all_records)
    else:
        resp = {"status": "success",
                "data": "no articles to insert"}

    if resp["status"] == "success":
        print(resp["data"])
    else:
        print("error: {}".format(resp["error"]))
    os.rmdir(DESTINATION_FOLDER)


def process_entity():
    """
    if the entity is tracked, we fetch and process the latest
    json based on date_to of the data. it's up to the company
    intelligence to make sure the data of all time periods
    are tracked.
    """
    pass


def index_articles():
    """
    indexes articles by maintaining a relationship
    with the particular entity that is being
    tracked.
    * last_tracked is the date_to of the latest file
    * plug-in scripts based on source
    * updates tracking table when daily news processed
    * auto-updates from the last date it was tracked
    """

    # setup connection to database
    global_init()

    try:
        objects = EntityIndex.objects().filter(actively_tracking=True)
    except Exception as e:
        print(e)
        return

    new_entities = [
        obj for obj in objects if obj.history_processed == False]

    # old_entities = [
    #     obj for obj in objects if obj.last_tracked == True]

    # print(len(history_objects), len(daily_objects))
    process_new_entity_data(new_entities)


if __name__ == "__main__":
    index_articles()
