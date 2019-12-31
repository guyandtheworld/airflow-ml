import hashlib
import json
import os

from .data.entity import EntityIndex
from .data.article import Article
from .data import source_processor
from typing import List


DESTINATION_FOLDER = "temp"


def create_company(entities: dict) -> List[EntityIndex]:
    """
    creates company by bulk
    """

    to_insert = []

    for entity in entities:
        new_entity = EntityIndex(
            entity_id=entity["entity_id"],
            entity_legal_name=str(entity["entity_legal_name"]),
            is_company=entity["is_company"])
        to_insert.append(new_entity)

    print("inserting {} items into db".format(len(entities)))

    try:
        resp = EntityIndex.objects().insert(to_insert)
        resp = {"status": "success",
                "data": [entity.entity_legal_name for entity in resp]
                }
    except ConnectionError as e:
        resp = {"status": "error",
                "error": e}
    except Exception as e:
        resp = {"status": "error",
                "error": e}
    return resp


def write_article(records: List[Article]) -> dict:
    """
    store articles in mongo db
    """
    try:
        resp = Article.objects().insert(records)
        resp = {"status": "success",
                "data": "inserted {} articles into db".format(len(records))
                }
    except ConnectionError as e:
        resp = {"status": "error",
                "error": e}
    except Exception as e:
        resp = {"status": "error",
                "error": e}
    return resp


def update_entities(entities: List[EntityIndex]) -> dict:
    """
    update the entities in mongo db
    """
    try:
        for obj in entities:
            obj.save()
        resp = {"status": "success",
                "data": "inserted {} articles into db".format(len(entities))
                }
    except ConnectionError as e:
        resp = {"status": "error",
                "error": e}
    except Exception as e:
        resp = {"status": "error",
                "error": e}
    return resp


def index_articles(record: dict, metadata):
    """
    depending on the source the article is from
    we pre-process the json and write it onto
    the Article object and save it

    ## args
    * record: a remote raw json file storage
    * metadata: details regarding the company and the source

    ## returns
    processed articles based on MongoDB Article model
    """
    with open(record["file"], "r") as fp:
        data = json.load(fp)
        processor = getattr(source_processor, record["source"])
        processed_records = processor(data, metadata)
    return processed_records


def process_company_json(record: dict, bucket, metadata: dict):
    """
    fetches file and stores it locally to fetch and preprocess
    returns the processed articles

    ## args
    * record: a remote raw json file storage
    * bucket: Google Bucket Instance
    * metadata: details regarding the company and the source

    ## returns
    processed articles based on MongoDB Article model
    """
    blob = bucket.blob(record["file"])
    hash_f = hashlib.sha1(record["file"].encode("UTF-8")).hexdigest()
    file_path = "{}/{}.json".format(DESTINATION_FOLDER, hash_f)
    blob.download_to_filename(file_path)
    record["file"] = file_path
    processed_records = index_articles(record, metadata)
    os.remove(file_path)
    return processed_records
