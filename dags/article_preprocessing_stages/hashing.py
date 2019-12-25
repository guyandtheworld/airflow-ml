import datetime

from google.cloud import storage
from google.cloud.storage import Blob

from data.mongo_setup import global_init
from data.entity_article import EntityArticleIndex
from utils import create_company


def index_company():
    """
    * indexes new companies on our database
    * keeps track of the last date we tracked our entity
    """

    # setup connection to database
    global_init()

    # fetch companies being tracked on the bucket
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs("raw-alrt-ai")
    except Exception as e:
        print(e)
        return

    # fetch the companies being tracked on our mongodb
    try:
        objects = EntityArticleIndex.objects().filter(actively_tracking=True)
    except Exception as e:
        print(e)
        return

    entities_on_bucket = set([x.name.split("/")[0] for x in blobs])
    print("entities inside bucket: {}".format(len(entities_on_bucket)))
    entities_tracked = set([entity.entity_name for entity in objects])
    print("entities tracked on db: {}".format(len(entities_tracked)))
    entities_to_be_tracked = entities_on_bucket - entities_tracked
    print("entities not tracked: {}".format(len(entities_to_be_tracked)))

    if len(entities_to_be_tracked) > 0:
        skeletons = []
        for entity in entities_to_be_tracked:
            obj = {"entity_name": entity,
                   "last_tracked": datetime.datetime.now(),
                   "is_company": True
                   }
            skeletons.append(obj)

        response = create_company(skeletons)
        print(response)
    else:
        print("no items to insert")


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
    pass


def greet():
    print('Writing in file')


if __name__ == "__main__":
    index_company()
