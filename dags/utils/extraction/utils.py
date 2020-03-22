import uuid
import logging

from utils.data.postgres_utils import connect, insert_values


logging.basicConfig(level=logging.INFO)


def get_types_ids(labels):
    """
    Check if there are new types of entities, if there is
    add it to the entity type table and then return the
    UUID for all the entities
    """
    ids_str = "', '".join(labels)
    ids_str = "('{}')".format(ids_str)

    query = """
            select uuid, name from apis_entitytype
            where name in {}
            """.format(ids_str)

    results = connect(query, verbose=False)
    existing_types = [item[1] for item in results]

    new_types = set(labels) - set(existing_types)

    if new_types:
        logging.info("inserting: {}".format(", ".join(new_types)))        
        insert_query = """
                    INSERT INTO public.apis_entitytype
                    (uuid, "name") VALUES(%s, %s);"""

        values = []
        for etype in new_types:
            values.append((str(uuid.uuid4()), etype))

        insert_values(insert_query, values)

    results = connect(query, verbose=False)
    types = {item[1]: item[0] for item in results}

    return types


def insert_story_entity_ref(values):
    query = """
            INSERT INTO public.apis_storyentityref
            (uuid, "name", "typeID_id")
            VALUES(%s, %s, %s);
            """

    insert_values(query, values)


def insert_story_entity_map(values):
    query = """
            INSERT INTO public.apis_storyentitymap
            (uuid, "entityID_id", "storyID_id")
            VALUES(%s, %s, %s);
            """

    insert_values(query, values)
