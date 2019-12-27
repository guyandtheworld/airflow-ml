import os

from data.entity import EntityIndex
from typing import List


DESTINATION_FOLDER = "tmp"


def create_company(entities: dict) -> List[EntityIndex]:
    """
    creates company by bulk
    """

    to_insert = []

    for entity in entities:
        new_entity = EntityIndex(
            entity_id=entity["entity_id"],
            entity_legal_name=str(entity["entity_legal_name"]),
            last_tracked=entity["last_tracked"],
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


def process_company_json(details: dict, bucket):
    blob = bucket.blob(details["file"])
    hashed_file = "dafskljfa"
    blob.download_to_filename(
        "{}/{}.json".format(DESTINATION_FOLDER, hashed_file))
    # os.remove(hashed_file)
