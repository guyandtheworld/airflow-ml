from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.storage import Blob

from data.entity import EntityIndex
from data.mongo_setup import global_init


def get_bucket_files():
    # fetch companies being tracked on the bucket
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs("alrt-ai-ps")
    except Exception as e:
        print(e)
        return


def process_new_entity_data():
    """
    it processes the history if the entity is not tracked.
    fetches all files within the corresponding dir - process
    it and change last tracked to the latest date_to.
    """
    pass


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

    history_objects = [
        obj for obj in objects if obj.history_processed == False]

    daily_objects = [
        obj for obj in objects if obj.last_tracked == True]

    print(len(history_objects), len(daily_objects))


if __name__ == "__main__":
    index_articles()
