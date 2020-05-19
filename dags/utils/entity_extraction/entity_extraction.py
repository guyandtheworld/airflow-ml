import logging
import pandas as pd
import time

from timeit import default_timer as timer
from google.cloud import language_v1
from utils.data.postgres_utils import connect
from .utils import (get_articles,
                    get_entities,
                    analyze_entities)


logging.basicConfig(level=logging.INFO)


def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


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
        if isEnglish(text):
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
            select "parentID_id", name from entity_alias
            where name in {}
            """.format(ids_str)

    # fetch uuid of existing items and new items and add to merged_df
    # if exists in apis_story_ref, just add ref in map table
    results = connect(query, verbose=False)

    logging.info("{} existing entity_alias found".format(len(results)))

    story_entity_ref_df = pd.DataFrame(
        results, columns=["entity_ref_id", "entity_name"])

    # drop duplicates
    story_entity_ref_df = story_entity_ref_df.drop_duplicates(
        subset='entity_name', keep="first")

    merged_df = pd.merge(merged_df, story_entity_ref_df,
                         how='left', left_on="text", right_on="entity_name")

    merged_df["wiki"].fillna("", inplace=True)

    merged_df.to_csv("merged_df.csv", index=False)
    df.to_csv("df.csv", index=False)
    story_entity_df.to_csv("story_entity_df.csv", index=False)
