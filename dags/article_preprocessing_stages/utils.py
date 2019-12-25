from data.entity import EntityIndex
from typing import List


def create_company(entities: dict) -> List[EntityIndex]:
    """
    creates company by bulk
    """

    to_insert = []

    for entity in entities:
        new_entity = EntityIndex(
            entity_search_name=str(entity["entity_search_name"]),
            last_tracked=entity["last_tracked"],
            is_company=entity["is_company"])
        to_insert.append(new_entity)

    print("inserting {} items into db".format(len(entities)))

    try:
        resp = EntityIndex.objects().insert(to_insert)
        resp = {"status": "success",
                "data": [entity.entity_search_name for entity in resp]
                }
    except ConnectionError as e:
        resp = {"status": "error",
                "error": e}
    except Exception as e:
        resp = {"status": "error",
                "error": e}
    return resp
