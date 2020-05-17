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


def insert_values():

    merged_df = pd.read_csv("merged_df.csv")
    df = pd.read_csv("df.csv")
    story_entity_df = pd.read_csv("story_entity_df.csv")

    # find and input new types
    TYPES = get_types_ids(list(story_entity_df["label"].unique()))

    # prepare entity ids and new entities to insert
    new = {}
    for index, row in merged_df.iterrows():
        if pd.isnull(row["entity_id"]):
            if pd.isnull(row["entity_ref_id"]):
                if row["text"] not in new:
                    new[row["text"]] = dict(
                        uuid=str(uuid.uuid4()),
                        type=row["label"],
                        wiki=row["wiki"]
                    )
                    merged_df.at[index,
                                 "entity_id"] = new[row["text"]]["uuid"]
                else:
                    merged_df.at[index,
                                 "entity_id"] = new[row["text"]]["uuid"]
            else:
                merged_df.at[index,
                             "entity_id"] = merged_df.loc[index]["entity_ref_id"]

    for key, value in new.items():
        obj = (
            value["uuid"],
            key,
            TYPES[value["type"]],
            value["wiki"],
            True,
            str(datetime.utcnow())
        )
        STORY_REF_INPUTS.append(obj)

    columns_to_drop = ["legal_name", "wiki", "label",
                       "entity_ref_id", "entity_name", "text"]
    merged_df.drop(columns_to_drop, axis=1, inplace=True)

    # generate uuids for story_map
    uuids = []
    for i in range(len(merged_df)):
        uuids.append(str(uuid.uuid4()))

    logging.info("{}".format(merged_df.isnull().values.any()))

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
