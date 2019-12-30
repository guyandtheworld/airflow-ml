import json
import os

from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.storage import Blob

from indexing_stages.data.entity import EntityIndex
from indexing_stages.data.mongo_setup import global_init
from indexing_stages.utils import process_company_json, write_article, \
    update_entities

# should store sources in the database
SOURCES = ["gdelt", "google_news"]
BUCKET_NAME = "alrt-ai-ps"
DESTINATION_FOLDER = "temp"


def process_entities(records, entities, storage_client):

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
        print("updating entity status")
        update_entities(entities)
        print(resp["data"])
    else:
        print("error: {}".format(resp["error"]))
    os.rmdir(DESTINATION_FOLDER)


def filter_new_entities(entities, storage_client):
    """
    it processes the history if the entity is not tracked.
    fetches all files within the corresponding dir - process
    it and change last tracked to the latest date_to.

    # to do
    we need to update entity_search_name, ticker, public
    and aliases once we fetch first set of data
    """

    records = []

    print("fetching new objects")
    for obj in entities:
        uid = obj.entity_id
        name = obj.entity_legal_name

        for source in SOURCES:
            prefix = "{}-{}/{}".format(uid, name, source)
            blobs = storage_client.list_blobs(
                BUCKET_NAME, prefix=prefix)

            for blob in blobs:
                record = {}

                # feels like this is a hack
                dates = blob.name.split("/")[-1]
                to_date = dates.strip(".json").split("Z-")[1]

                record["file"] = blob.name
                record["id"] = uid
                record["name"] = name
                record["source"] = source
                record["entity_object"] = obj
                records.append(record)

        # updating the latest_tracking info
        to_date = datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%SZ")
        obj.last_tracked = to_date
        obj.history_processed = True

    process_entities(records, entities, storage_client)


def filter_existing_entities(entities, storage_client):
    """
    if the entity is tracked, we fetch and process the latest
    json based on date_to of the data. it's up to the company
    intelligence to make sure the data of all time periods
    are tracked.
    """

    records = []

    print("fetching new objects")
    for obj in entities:
        print(obj.entity_legal_name)
        uid = obj.entity_id
        name = obj.entity_legal_name

        for source in SOURCES:
            prefix = "{}-{}/{}".format(uid, name, source)
            blobs = storage_client.list_blobs(
                BUCKET_NAME, prefix=prefix)

            for blob in blobs:
                record = {}

                dates = blob.name.split("/")[-1]
                tracking_dates = dates.strip(".json").split("Z-")
                from_date = tracking_dates[0]
                to_date = tracking_dates[1]
                from_date = datetime.strptime(
                    from_date, "%Y-%m-%dT%H:%M:%S")

                # files that are untracked
                if from_date >= obj.last_tracked:
                    record["file"] = blob.name
                    record["id"] = uid
                    record["name"] = name
                    record["source"] = source
                    record["entity_object"] = obj
                    records.append(record)

        to_date = datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%SZ")
        if to_date > obj.last_tracked:
            obj.last_tracked = to_date

    process_entities(records, entities, storage_client)


def index_articles():
    """
    indexes articles by maintaining a relationship
    with the particular entity that is being
    tracked.
    * last_tracked is the date_to of the latest file
    * plug-in scripts based on source
    * updates tracking attribute when daily news processed
    * auto-updates from the last date it was tracked
    """

    # setup connection to database
    global_init()

    print("loading storage client")
    storage_client = storage.Client()

    try:
        objects = EntityIndex.objects().filter(actively_tracking=True)
    except Exception as e:
        print(e)
        return

    new_entities = [
        obj for obj in objects if obj.history_processed == False]

    old_entities = [
        obj for obj in objects if obj.history_processed == True]

    print("new entires: {}".format(len(new_entities)))
    print("tracked entries: {}".format(len(old_entities)))
    filter_new_entities(new_entities, storage_client)
    filter_existing_entities(old_entities, storage_client)


if __name__ == "__main__":
    index_articles()
