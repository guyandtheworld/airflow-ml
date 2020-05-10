import uuid
import logging
import pandas as pd

from google.cloud.language_v1 import enums
from google.protobuf.json_format import MessageToDict

from utils.data.postgres_utils import (connect,
                                       insert_values,
                                       delete_values)


logging.basicConfig(level=logging.INFO)


def filter_entities(dict_obj, uuid):
    """
    Convert the Entity response into format for our database.

    Args:
        dict_obj - dictionary with entities
        uuid - uuid of the article
    """
    filtered_entities = []
    for entity in dict_obj["entities"]:
        if "type" in entity["mentions"][0] and \
                entity["mentions"][0]["type"] == "PROPER":
            filtered_entities.append(entity)

    filtered = []
    for entity in filtered_entities:
        obj = {}
        obj["uuid"] = uuid
        obj["name"] = entity["name"].replace("'", "")
        obj["type"] = entity["type"]
        obj["salience"] = entity["salience"]
        if "metadata" in entity and "wikipedia_url" in entity["metadata"]:
            obj["wikipedia"] = entity["metadata"]["wikipedia_url"]
        else:
            obj["wikipedia"] = ""
        if "mentions" in entity:
            obj["mentions"] = len(entity["mentions"])
        else:
            obj["mentions"] = 1
        filtered.append(tuple(obj.values()))
    return filtered


def analyze_entities(client, uuid, text_content):
    """
    Analyzing Entities in a String

    Args:
      text_content The text content to analyze
    """

    type_ = enums.Document.Type.PLAIN_TEXT

    language = "en"
    document = {"content": text_content, "type": type_, "language": language}

    encoding_type = enums.EncodingType.UTF8

    response = client.analyze_entities(document, encoding_type=encoding_type)

    dict_obj = MessageToDict(response)
    dict_obj = filter_entities(dict_obj, uuid)
    return dict_obj


def get_articles():
    """
    Fetch all stories where body exists and we haven't done
    Entity Recognition from active Scenarios
    """
    query = """
                SELECT story.uuid, story.title, body.body FROM
                public.apis_story story
                LEFT JOIN
                (SELECT distinct "storyID_id" FROM public.apis_storyentitymap) entity
                ON story.uuid = entity."storyID_id"
                INNER JOIN (select "storyID_id", (array_agg(body))[1] as body
                from apis_storybody where status_code=200 group by "storyID_id") AS body
                ON story.uuid = body."storyID_id"
                WHERE entity."storyID_id" IS null
                AND "language" in ('english', 'US', 'CA', 'AU', 'IE')
                AND "scenarioID_id" in (SELECT uuid FROM apis_scenario as2 WHERE status = 'active')
                LIMIT 5000
            """

    response = connect(query)

    df = pd.DataFrame(response, columns=["uuid", "title", "body"])
    return df


def articles_without_entities(df, entity_df):
    """
    delete articles that doesn't have entities
    """
    df = df.merge(entity_df, how="left", left_on="uuid", right_on="story_uuid")
    df = df[df.isnull().any(axis=1)]

    logging.info(df.head(20)["body"].values)

    if len(df) > 0:
        logging.info("deleting {} values".format(df["uuid"].nunique()))
        ids_str = "', '".join(df["uuid"].unique())
        ids_str = "('{}')".format(ids_str)

        QUERIES = ['delete from apis_bucketscore ab where "storyID_id" in {}',
                   'delete from apis_entityscore ae where "storyID_id" in {}',
                   'delete from apis_storybody ae where "storyID_id" in {}',
                   'delete from apis_storysentiment ae where "storyID_id" in {}',
                   'delete from apis_story as2 where uuid in {}']

        for query in QUERIES:
            delete_values(query, ids_str)


def get_entities():

    # Fetch all entity aliases that we have in our storage
    query = """
        select entity.uuid, entity.name as legal_name from
        public.apis_entity entity
        full outer join
        where "manualEntry"=true
        and "entryVerified"=true;
        """

    results = connect(query, verbose=False)

    insert_entity_into_entityref()

    entity_df = pd.DataFrame(
        results, columns=["entity_id", "legal_name"])
    return entity_df


def insert_entity_into_entityref():
    """
    fetch all entities that are not in entity_ref
    and input it into entity_ref

    if entity exists in api_entity table but not in api_story_entity_ref table
    with same UUID, add it to apis_storyentityref
    save all entity uuid, new and old to merged_df
    """
    query = """
            select entity.uuid, entity.name as legal_name,
            entity."typeID_id", "wikipedia", true
            from apis_entity entity where uuid not in
            (select ae.uuid from apis_entity ae
            inner join apis_storyentityref ar
            on ae.uuid = ar.uuid)
            and entity."entryVerified"=true
            """
    results = connect(query, verbose=False)
    logging.info("{} entities to insert to entityref".format(len(results)))
    insert_story_entity_ref(results)


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


def match_manual_entity_to_story(df):
    """
    Create a manual check of whether our pre-determined
    entities are in the story and create a link.
    """
    query = """
            select entity.uuid, entity.name as legal_name
            from apis_entity ae
            inner join apis_storyentityref entity
            on ae.uuid = entity.uuid
            """
    results = connect(query, verbose=False)
    df["text"] = df["title"] + df["body"]
    df["text"] = df["text"].str.lower()

    story_map_inputs = []

    for _, row in df.iterrows():
        for entity in results:
            if entity[1].lower() in row["text"]:
                data = (str(uuid.uuid4()),
                        entity[0],
                        row["uuid"],
                        1,
                        .5
                        )
                story_map_inputs.append(data)

    logging.info("{} manual relations found".format(len(story_map_inputs)))
    insert_story_entity_map(story_map_inputs)


def insert_story_entity_ref(values):
    query = """
            INSERT INTO public.apis_storyentityref
            (uuid, "name", "typeID_id", wikipedia, render)
            VALUES(%s, %s, %s, %s, %s);
            """

    insert_values(query, values)


def insert_story_entity_map(values):
    query = """
            INSERT INTO public.apis_storyentitymap
            (uuid, "entityID_id", "storyID_id", mentions, salience)
            VALUES(%s, %s, %s, %s, %s);
            """

    insert_values(query, values)
