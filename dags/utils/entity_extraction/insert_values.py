import logging
import uuid
import pandas as pd

from datetime import datetime
from .utils import (insert_story_entity_ref,
                    insert_story_entity_map,
                    get_types_ids,
                    match_manual_entity_to_story,
                    articles_without_entities)


logging.basicConfig(level=logging.INFO)

STORY_REF_INPUTS = []
STORY_MAP_INPUTS = []
ENTITY_ALIAS_INPUTS = []


def insert_values():
    """
    Input mappings, parents and alias into corresponding tables.
    """
    merged_df = pd.read_csv("merged_fuzzy_df.csv")
    df = pd.read_csv("df.csv")
    story_entity_df = pd.read_csv("story_entity_df.csv")

    merged_df['entity_id'] = merged_df['entity_id'].apply(str)

    # find and input new types
    TYPES = get_types_ids(list(story_entity_df["label"].unique()))

    new_parents = {}
    new_alias = {}

    # if score = -2, it needs new alias as well as new parents
    for index, row in merged_df[merged_df["score"] == -2].iterrows():

        # create a new parent
        if row["text"] not in new_parents:
            new_parents[row["text"]] = [
                str(uuid.uuid4()),
                row["text"],
                TYPES[row["label"]],
                row["wiki"],
                True,
                str(datetime.utcnow())
            ]

        merged_df.at[index,
                     "entity_id"] = new_parents[row["text"]][0]

        # add alias with corresponding parent ID
        if row["text"] not in new_alias:
            new_alias[row["text"]] = [
                str(uuid.uuid4()),
                row["text"],
                row["wiki"],
                row["score"],
                str(datetime.utcnow()),
                new_parents[row["text"]][0],
                TYPES[row["label"]]
            ]

    for index, row in merged_df[merged_df["score"] >= -1].iterrows():
        # if score >= -1, it needs new alias
        if row["text"] not in new_alias:
            new_alias[row["text"]] = [
                str(uuid.uuid4()),
                row["text"],
                row["wiki"],
                row["score"],
                str(datetime.utcnow()),
                row["entity_ref_id"],
                TYPES[row["label"]]
            ]

    # if already matched, write story_entity_id into entity_id for mapping
    for index, row in merged_df[merged_df["score"].isnull()].iterrows():
        merged_df.at[index,
                     "entity_id"] = row["entity_ref_id"]

    for _, value in new_parents.items():
        STORY_REF_INPUTS.append(value)

    for _, value in new_alias.items():
        ENTITY_ALIAS_INPUTS.append(value)

    print("parents: ", len(STORY_REF_INPUTS))
    print("alias: ", len(ENTITY_ALIAS_INPUTS))

    columns_to_drop = ["legal_name", "wiki", "label",
                       "entity_ref_id", "entity_name", "text"]
    merged_df.drop(columns_to_drop, axis=1, inplace=True)

    # generate uuids for story_map
    uuids = []
    for _ in range(len(merged_df)):
        uuids.append(str(uuid.uuid4()))

    print(merged_df.head())
    logging.info("check na {}".format(merged_df.isnull().values.any()))

    # input new_entites to table
    # using entity UUID and story UUID to apis_story_enity_map table

    merged_df["uuid"] = uuids
    merged_df["created_at"] = str(datetime.utcnow())

    merged_df = merged_df[["uuid", "entity_id",
                           "story_uuid", "mentions",
                           "salience", "created_at"]]

    STORY_MAP_INPUTS = [tuple(row)
                        for row in merged_df.itertuples(index=False)]

    # see if there are apis_entity elements in the stories
    # match_manual_entity_to_story(df)

    # insert_story_entity_ref(STORY_REF_INPUTS)
    # insert_story_entity_map(STORY_MAP_INPUTS)

    # logging.info("finished")

    # logging.info("delete articles without entities")
    # articles_without_entities(df, story_entity_df)
