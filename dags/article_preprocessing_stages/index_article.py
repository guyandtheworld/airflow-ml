from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.storage import Blob

from data.entity import EntityIndex
from data.mongo_setup import global_init


def index_articles():
    """
    indexes articles by maintaining a relationship
    with the particular entity that is being
    tracked.
    * if history not processed, indexes history, change status
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

    min_date = datetime.now() - timedelta(days=1)
    daily_objects = [
        obj for obj in objects if obj.last_tracked < min_date]

    print(len(history_objects), len(daily_objects))

    # fetch companies being tracked on the bucket
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs("raw-alrt-ai")
    except Exception as e:
        print(e)
        return


if __name__ == "__main__":
    index_articles()
