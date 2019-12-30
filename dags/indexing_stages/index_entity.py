import datetime

from google.cloud import storage
from google.cloud.storage import Blob

from indexing_stages.data.mongo_setup import global_init
from indexing_stages.data.entity import EntityIndex
from indexing_stages.utils import create_company


BUCKET_NAME = "alrt-ai-ps"


def index_entities():
    """
    * indexes new companies on our database
    * keeps track of the last date we tracked our entity
    """

    # setup connection to database
    global_init()

    # fetch companies being tracked on the bucket
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME)
    except Exception as e:
        print(e)
        return

    # fetch the companies being tracked on our mongodb
    try:
        objects = EntityIndex.objects().filter(actively_tracking=True)
    except Exception as e:
        print(e)
        return

    entities_on_bucket = set([x.name.split("/")[0] for x in blobs])
    entities_id_bucket = {int(x.split("-")[0]): x.split("-")[1]
                          for x in entities_on_bucket}

    print("entities inside bucket: {}".format(len(entities_id_bucket)))
    entities_tracked = set([entity.entity_id for entity in objects])
    print("entities tracked on db: {}".format(len(entities_tracked)))
    entities_to_be_tracked = set(entities_id_bucket.keys()) - entities_tracked
    print("entities not tracked: {}".format(len(entities_to_be_tracked)))

    if len(entities_to_be_tracked) > 0:
        skeletons = []
        for entity_id in entities_to_be_tracked:
            obj = {
                "entity_id": entity_id,
                "entity_legal_name": entities_id_bucket[entity_id],
                "is_company": True
            }
            skeletons.append(obj)
        response = create_company(skeletons)
        print(response)
    else:
        print("no items to insert")


if __name__ == "__main__":
    index_company()
