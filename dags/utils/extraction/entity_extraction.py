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

    TYPE = "yo"
    STORY_REF_INPUTS = []
    STORY_MAP_INPUTS = []

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

    results = connect(query)

    entity_df = pd.DataFrame(results, columns=["entity_id", "legal_name", "alias"])

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

    results = connect(query)
    entity_ids_in_storyref = [x[0] for x in results]

    # if entity exists in api_entity table but not in api_story_entity_ref table
    # with same UUID, add it to apis_storyentityref
    # save all entity uuid, new and old to merged_df
    to_insert_into_storyref = list(set(ent_ids_to_check)
                                   - set(entity_ids_in_storyref))
    for euuid in to_insert_into_storyref:
        entity_name = merged_df[merged_df["entity_id"] == euuid]["legal_name"].iloc[0]
        STORY_REF_INPUTS.append((euuid, entity_name, TYPE))

    print(STORY_REF_INPUTS)

    # if it doesn't exists in apis_entity table, and is new generate new uuid
    # and add new entities to apis_storyentityref
    check_label_in_story_ref = set(merged_df[merged_df.isna().any(axis=1)]["text"])

    ids_str = "', '".join(check_label_in_story_ref)
    ids_str = "('{}')".format(ids_str)

    query = """
            select uuid, name from apis_storyentityref
            where name in {}
            """.format(ids_str)

    # fetch uuid of existing items and new items and add to merged_df
    # if exists in apis_story_ref, just add ref in map table
    results = connect(query, verbose=False)
    story_entity_ref_df = pd.DataFrame(results, columns=["entity_ref_id", "entity_name"])

    merged_df = pd.merge(merged_df, story_entity_ref_df,
                         how='left', left_on="text", right_on="entity_name")

    new_entities = {}
    for index, row in merged_df.iterrows():
        if pd.isnull(row["entity_id"]) and pd.isnull(row["entity_ref_id"]):
            if row["text"] not in new_entities:
                new_entities[row["text"]] = str(uuid.uuid4())
                merged_df.loc[index]["entity_id"] = new_entities[row["text"]]
            else:
                merged_df.loc[index]["entity_id"] = new_entities[row["text"]]
        else:
            merged_df.loc[index]["entity_id"] = merged_df.loc[index]["entity_ref_id"]

    print(new_entities)

    for key, value in new_entities.items():
        STORY_REF_INPUTS.append((value, key, TYPE))

    print(merged_df.head())

    # input new_entites (without duplicates) to table

    # using entity UUID and story UUID to apis_story_enity_map table

    # test with counts how it works afterwards

    logging.info("finished")
