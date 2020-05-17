import pandas as pd
import numpy as np
from rapidfuzz import process, utils, fuzz

from tqdm.notebook import tqdm
tqdm.pandas()


def process_entity(entity):
    stop_words = ['group', 'ltd', 'corporation', 'company', 'the', 'corp']
    processed_entity = ' '.join(
        [word for word in entity.split() if word.lower() not in stop_words]).lower()
    return processed_entity


def process_fuzzy(df):
    count = 0
    results = {}
    processed_orgs = {row['uuid']: utils.default_process(
        row['name']) for _, row in df.iterrows()}

    for index, entity in df.iterrows():

        if count % 100 == 0:
            print(count)
        count += 1

        wikilink = entity.wikipedia

        # match wikilink to any other wikilink, and if it has a alias, match it to that O(1)
        if wikilink and wikilink in df.wikipedia.unique():
            match = df[df['wikipedia'] == wikilink].index[0]
            df.loc[index, 'alias'] = match
            df.loc[index, 'score'] = -1
            continue

        # try matching processed entity.name with all of the entities
        best_match = process.extractOne(
            entity.processed, processed_orgs, processor=None, scorer=fuzz.token_set_ratio, score_cutoff=70)

        if best_match and best_match[0] != entity.uuid and \
                entity.typeID_id == df.loc[best_match[0]].typeID_id:
            df.loc[index, 'alias'] = best_match[0]
            df.loc[index, 'score'] = best_match[1]
        else:
            df.loc[index, 'alias'] = entity.uuid
            df.loc[index, 'score'] = -2
    return df


def fuzzy_matching():
    df['processed'] = df.name.map(process_entity)
