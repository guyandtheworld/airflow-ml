import uuid
import logging
import en_core_web_md
import pandas as pd

from utils.data.postgres_utils import connect, insert_values


nlp = en_core_web_md.load()

logging.basicConfig(level=logging.INFO)

entity_types_to_save = ["PERSON", "ORG", "GPE", "EVENT",
                        "FAC", "LOC", "FACILITY", "NORP",
                        "PRODUCT"]


def named_entity_recognition(uuid: str, text: str) -> list:
    """
    recognizes entities in the given text
    """

    document = nlp(text)
    out = []
    for x in document.ents:
        if x.label_ in entity_types_to_save:
            out.append((uuid, x.text, x.label_))
    return out


def get_articles():

    # fetch all stories where body exists and we haven't done
    # entity recognition
    query = """
                SELECT story.uuid, story.title, body.body FROM
                public.apis_story story
                LEFT JOIN
                (SELECT distinct "storyID_id" FROM public.apis_storyentitymap) entity
                ON story.uuid = entity."storyID_id"
                LEFT JOIN
                public.apis_storybody AS body
                ON story.uuid = body."storyID_id"
                WHERE entity."storyID_id" IS null
                and status_code=200
                AND body IS NOT NULL
                LIMIT 20000
            """

    response = connect(query)

    df = pd.DataFrame(response, columns=["uuid", "title", "body"])
    return df


def get_entities():

    # Fetch all entity aliases that we have in our storage
    query = """
        select entity.uuid, entity.name as legal_name, alias.name as alias from
        public.apis_entity entity
        full outer join
        public.apis_alias alias
        on entity.uuid = alias."entityID_id"
        where "manualEntry"=true
        and "entryVerified"=true
        and alias is not null;
        """

    results = connect(query, verbose=False)

    entity_df = pd.DataFrame(results, columns=["entity_id", "legal_name", "alias"])
    return entity_df


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
