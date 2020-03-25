import logging
import uuid
import pandas as pd

from utils.data.postgres_utils import connect
from .utils import (get_articles,
                    get_entities,
                    get_types_ids,
                    insert_story_entity_ref,
                    insert_story_entity_map,
                    named_entity_recognition)


logging.basicConfig(level=logging.INFO)

STORY_REF_INPUTS = []
STORY_MAP_INPUTS = []


def extract_entities():
    """
    extracts entities using spaCy from the body
    of the article
    """

    df = get_articles()

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

    # find and input new types
    TYPES = get_types_ids(list(story_entity_df["label"].unique()))

    # fetch and add existing entities in api_entity
    entity_df = get_entities()

    # unique values by using combination of article uuid and the text
    merged_df = pd.merge(story_entity_df, entity_df,
                         how='left', left_on="text", right_on="legal_name")

    ent_ids_to_check = merged_df[~merged_df.isna().any(axis=1)]["entity_id"].unique()

    if len(ent_ids_to_check) > 0:
        ids_str = "', '".join(ent_ids_to_check)
        ids_str = "('{}')".format(ids_str)

        # check if apis_entity objects exists in apis_storyentityref
        query = """
                select uuid from apis_storyentityref
                where uuid in {}
                """.format(ids_str)

        results = connect(query, verbose=False)
        logging.info("{} existing entity found".format(len(results)))
    else:
        results = []

    entity_ids_in_storyref = [x[0] for x in results]

    # if entity exists in api_entity table but not in api_story_entity_ref table
    # with same UUID, add it to apis_storyentityref
    # save all entity uuid, new and old to merged_df
    to_insert_into_storyref = list(set(ent_ids_to_check)
                                   - set(entity_ids_in_storyref))
    for euuid in to_insert_into_storyref:
        entity_name = merged_df[merged_df["entity_id"] == euuid]["legal_name"].iloc[0]
        entity_type = merged_df[merged_df["entity_id"] == euuid]["label"].iloc[0]
        STORY_REF_INPUTS.append((euuid, entity_name, TYPES[entity_type]))
        logging.info("existing entity: {}".format((entity_name, entity_type)))

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

    logging.info("{} existing storyentityref found".format(len(results)))

    story_entity_ref_df = pd.DataFrame(results, columns=["entity_ref_id", "entity_name"])

    merged_df = pd.merge(merged_df, story_entity_ref_df,
                         how='left', left_on="text", right_on="entity_name")

    new_entities = {}
    for index, row in merged_df.iterrows():
        if pd.isnull(row["entity_id"]):
            if pd.isnull(row["entity_ref_id"]):
                if row["text"] not in new_entities:
                    new_entities[row["text"]] = str(uuid.uuid4())
                    merged_df.loc[index]["entity_id"] = new_entities[row["text"]]
                else:
                    merged_df.loc[index]["entity_id"] = new_entities[row["text"]]
            else:
                merged_df.loc[index]["entity_id"] = merged_df.loc[index]["entity_ref_id"]

    for key, value in new_entities.items():
        entity_type = merged_df[merged_df["entity_id"] == value]["label"].iloc[0]
        STORY_REF_INPUTS.append((value, key, TYPES[entity_type]))

    columns_to_drop = ["legal_name", "alias", "entity_ref_id", "entity_name", "text", "label"]
    merged_df.drop(columns_to_drop, axis=1, inplace=True)

    # input new_entites to table

    # using entity UUID and story UUID to apis_story_enity_map table

    uuids = []
    for i in range(len(merged_df)):
        uuids.append(str(uuid.uuid4()))

    logging.info("{}".format(merged_df.isnull().values.any()))

    merged_df["uuid"] = uuids
    merged_df = merged_df[["uuid", "entity_id", "story_uuid"]]

    STORY_MAP_INPUTS = list(merged_df.to_records(index=False))

    insert_story_entity_ref(STORY_REF_INPUTS)
    insert_story_entity_map(STORY_MAP_INPUTS)

    logging.info("finished")
