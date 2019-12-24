from google.cloud import storage
from google.cloud.storage import Blob


def index_company():

    storage_client = storage.Client()
    blobs = storage_client.list_blobs("raw-alrt-ai")

    entities_tracked = set([x.name.split("/")[0] for x in blobs])
    print(entities_tracked)


def greet():
    print('Writing in file')


if __name__ == "__main__":
    index_company()
