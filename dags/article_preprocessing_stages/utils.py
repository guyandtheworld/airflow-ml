from data.entity_article import EntityArticleIndex
from typing import List


def create_company(entities: dict) -> List[EntityArticleIndex]:
    """
    creates company by bulk
    """

    to_insert = []

    for entity in entities:
        new_entity = EntityArticleIndex(
            entity_name=str(entity["entity_name"]),
            last_tracked=entity["last_tracked"],
            is_company=entity["is_company"])
        to_insert.append(new_entity)

    print("inserting {} items into db".format(len(entities)))

    try:
        resp = EntityArticleIndex.objects().insert(to_insert)
        resp = {"status": "success",
                "data": [entity.entity_name for entity in resp]
                }
    except ConnectionError as e:
        resp = {"status": "error",
                "error": e}
    except Exception as e:
        resp = {"status": "error",
                "error": e}
    return resp
