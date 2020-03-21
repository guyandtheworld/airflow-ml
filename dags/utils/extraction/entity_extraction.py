import json
import logging
import uuid
import en_core_web_sm

import pandas as pd

from collections import defaultdict
from datetime import datetime
from utils.data.postgres_utils import connect, insert_values


nlp = en_core_web_sm.load()

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


def extract_entities():
    """
    extracts entities using spaCy from the body
    of the article
    """

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
                LIMIT 200
            """

    response = connect(query)

    df = pd.DataFrame(response, columns=["uuid", "title", "body"])

    count = 1
    values = []
    logging.info("extracting entities from {} articles".format(len(df)))

    for index, row in df.iterrows():
        text = "{} {}".format(row["title"], row["body"])
        entities = named_entity_recognition(row["uuid"], text)
        values += entities

        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    story_entity_df = pd.DataFrame(values, columns=["story_uuid", "text", "label"])
    print(story_entity_df.head())
    print(story_entity_df.shape)

    # Fetch all entity aliases that we have in our storage
    query = """
        select entity.uuid, alias.name as alias from
        public.apis_entity entity
        full outer join
        public.apis_alias alias
        on entity.uuid = alias."entityID_id"
        where "manualEntry"=true
        and "entryVerified"=true
        and alias is not null;
    """

    results = connect(query)

    entity_df = pd.DataFrame(results, columns=["entity_id", "alias"])

    # unique values by using combination of article uuid and the text
    merged_df = pd.merge(story_entity_df, entity_df,
                         how='left', left_on="text", right_on="alias")

    ent_ids_to_check = merged_df[~merged_df.isna().any(axis=1)]["entity_id"].unique()

    ids_str = "', '".join(ent_ids_to_check)
    ids_str = "('{}')".format(ids_str)

    # check if apis_entity objects exists in apis_storyentityref
    query = """
            select uuid from apis_storyentityref
            where uuid in {}
            """.format(ids_str)

    # if entity exists in api_entity table but not in api_story_entity_ref table
    # with same UUID, add it to apis_storyentityref
    # save all entity uuid, new and old to merged_df
    pass

    # if it doesn't exists in apis_entity table, and is new generate new uuid
    # and add new entities to apis_storyentityref
    # fetch uuid of existing items and new items and add to merged_df
    # if exists in apis_entity_table, just add ref in map table
    pass
    merged_df[merged_df.isna().any(axis=1)]

    # add corresponding UUIDs in map table
    # Then using entity UUID and story UUID to apis_story_enity_map table
    pass

    logging.info("finished")
