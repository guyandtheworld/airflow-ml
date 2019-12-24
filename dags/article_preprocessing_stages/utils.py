import datetime


from data import mongo_setup
from data.entity_article import EntityArticleIndex


def create_company(name: str, is_company: bool) -> EntityArticleIndex:
    # need to do it only once in our application
    mongo_setup.global_init()

    entity = EntityArticleIndex()
    entity.entity_name = name
    entity.last_tracked = datetime.datetime.now()
    entity.is_company = is_company

    entities = []
    for entity in range(5):
        new_entity = EntityArticleIndex(
            entity_name=str(entity), last_tracked=datetime.datetime.now())
        entities.append(new_entity)

    EntityArticleIndex.objects().insert(entities)

    # try:
    #     entity.save()
    # except Exception as e:
    #     resp = {"status": "connection error"}
    #     print(resp)
    #     print(e)

    return entity


if __name__ == "__main__":
    create_company(name="asdfads", is_company=True)
