import logging
import pandas as pd
import numpy as np

from rapidfuzz import process, utils, fuzz
from tqdm.notebook import tqdm
from utils.data.postgres_utils import connect


tqdm.pandas()

logging.basicConfig(level=logging.INFO)


def process_entity(entity):
    stop_words = ['group', 'ltd', 'corporation', 'company', 'the', 'corp']
    processed_entity = ' '.join(
        [word for word in entity.split() if word.lower() not in stop_words]).lower()
    return processed_entity


def process_fuzzy(merged_df, alias):
    """
    Uses Fuzzy matching to check if any of the unmatched entities
    can be matched using either wiki-links or fuzzy matching.
    """

    count = 0
    alias['name'] = alias['name'].apply(str)
    processed_orgs = {row['parent']: utils.default_process(
        row['name']) for _, row in alias.iterrows()}

    all_wiki_links = alias.wikipedia.unique()

    # Fetch all the entities that haven't been matched
    for index, entity in merged_df[merged_df["entity_ref_id"].isnull()].iterrows():

        if count % 100 == 0:
            logging.info(count)
        count += 1

        wikilink = entity.wiki

        # match wikilink to any other wikilink, and if it has a alias, match it to that O(1)
        if wikilink and wikilink in all_wiki_links:
            match = alias[alias['wikipedia'] == wikilink]['parent'][0]
            merged_df.loc[index, 'entity_ref_id'] = match
            merged_df.loc[index, 'score'] = -1
            continue

        # try matching processed entity.name with all of the entities
        best_match = process.extractOne(
            entity.text, processed_orgs, processor=process_entity,
            scorer=fuzz.token_set_ratio, score_cutoff=70)

        # if match found and is of the same type

        if best_match and \
                entity.label == alias[alias["parent"] == best_match[0]].any().type:
            merged_df.loc[index, 'entity_ref_id'] = best_match[0]
            merged_df.loc[index, 'score'] = best_match[1]
        else:
            merged_df.loc[index, 'score'] = -2
    return merged_df


def fuzzy_matching():
    """
    Score
    * -1: Matched using Wikipedia
    * -2: Should be parent - create parent and corresponding Alias
    * 0 - 100: Match found using Fuzzy
    """
    results = connect(
        'select name, wikipedia, "parentID_id", "typeID_id" from entity_alias')

    alias = pd.DataFrame(results, columns=[
        'name', 'wikipedia', 'parent', 'type'])

    logging.info("Starting Fuzzy matching.")
    merged_df = pd.read_csv("merged_df.csv")

    merged_df['score'] = np.nan

    merged_df = process_fuzzy(merged_df, alias)
    merged_df.to_csv("merged_fuzzy_df.csv", index=False)
