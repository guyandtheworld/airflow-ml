import logging
import uuid
import pandas as pd
import time

from timeit import default_timer as timer
from google.cloud import language_v1
from utils.data.postgres_utils import connect
from .utils import (get_articles,
                    get_entities,
                    get_types_ids,
                    insert_story_entity_ref,
                    insert_story_entity_map,
                    analyze_entities,
                    match_manual_entity_to_story,
                    articles_without_entities)


logging.basicConfig(level=logging.INFO)

STORY_REF_INPUTS = []
STORY_MAP_INPUTS = []


def extract_entities():
    """
    extracts entities using spaCy from the body
    of the article
    """

    df = get_articles()

    values = []
    logging.info("extracting entities from {} articles".format(len(df)))

    client = language_v1.LanguageServiceClient()

    limit = 500
    starttime = time.time()

    start = timer()
    for i, row in df.iterrows():
        # only process 500 articles per minute
        if not ((i + 1) % limit):
            sleeptime = starttime + 60 - time.time()
            if sleeptime > 0:
                time.sleep(sleeptime)
            starttime = time.time()

        text = "{} {}".format(row["title"], row["body"])[:999]
        entities = analyze_entities(client, row["uuid"], text)
        values += entities

        if not i % 100:
            logging.info("processed: {}".format(i))

    end = timer()
    logging.info("time elapsed: {}".format(end - start))

    story_entity_df = pd.DataFrame(
        values, columns=["story_uuid", "text", "label",
                         "salience", "wiki", "mentions"])

    # set character length of 196
    story_entity_df["text"] = story_entity_df["text"].str.slice(0, 196)

    # find and input new types
    TYPES = get_types_ids(list(story_entity_df["label"].unique()))

    # fetch and add existing entities in api_entity
    entity_df = get_entities()

    # unique values by using combination of article uuid and the text
    merged_df = pd.merge(story_entity_df, entity_df,
                         how='left', left_on="text", right_on="legal_name")

    # if it doesn't exists in apis_entity table, and is new generate new uuid
    # and add new entities to apis_storyentityref
    check_label_in_story_ref = set(
        merged_df[merged_df.isna().any(axis=1)]["text"])

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

    story_entity_ref_df = pd.DataFrame(
        results, columns=["entity_ref_id", "entity_name"])

    # drop duplicates
    story_entity_ref_df = story_entity_ref_df.drop_duplicates(
        subset='entity_name', keep="first")

    merged_df = pd.merge(merged_df, story_entity_ref_df,
                         how='left', left_on="text", right_on="entity_name")

    merged_df["wiki"].fillna("", inplace=True)

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
            True
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
    merged_df = merged_df[["uuid", "entity_id",
                           "story_uuid", "mentions", "salience"]]

    STORY_MAP_INPUTS = [tuple(row)
                        for row in merged_df.itertuples(index=False)]

    # see if there are apis_entity elements in the stories
    match_manual_entity_to_story(df)

    insert_story_entity_ref(STORY_REF_INPUTS)
    insert_story_entity_map(STORY_MAP_INPUTS)

    logging.info("finished")

    logging.info("delete articles without entities")
    articles_without_entities(df, story_entity_df)
