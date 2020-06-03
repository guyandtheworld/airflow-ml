import os
import logging
import pandas as pd

from .utils import (get_model_details,
                    get_scenario_articles,
                    get_bucket_ids,
                    insert_bucket_scores,
                    insert_entity_scores)


os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

logging.basicConfig(level=logging.INFO)


def semantic_search(text, keys):
    """
    See if the keys exist in the given article
    """

    for key in keys:
        find = text.lower().find(key)
        if (find != -1):
            return 1

    return 0


def search(text, bucket_keywords):
    prediction = {}
    for bucket in bucket_keywords.keys():
        if bucket != 'other':
            result = semantic_search(text, bucket_keywords[bucket])
            prediction[bucket] = result

    if 1 in list(prediction.values()):
        prediction["other"] = 0
    else:
        prediction["other"] = 1

    return prediction


def merge(row):
    """
    Merge title and body together
    """
    if not pd.isna(row["body"]):
        return "{} {}".format(row["title"], row["body"])
    return row["title"]


def sustainable():
    """
    Classify the text to find important events in SusFin Scenario
    """

    scenario = "Sustainable Finance"

    # fetch the latest model name from db
    results = get_model_details(scenario=scenario)

    print(results)
    if len(results) == 0:
        return

    bucket_ids, bucket_keywords = get_bucket_ids(scenario=scenario)

    print(bucket_keywords)

    model_uuid = results[0][0]

    articles = get_scenario_articles(
        model_uuid, scenario=scenario, body=True, article_count=5000)
    df = pd.DataFrame(articles, columns=[
                      "uuid", "title", "body",
                      "published_date", "sourceUUID", "entityUUID"])

    if len(df) > 0:
        df["text"] = df[["title", "body"]].apply(merge, axis=1)

    df.drop(['title', 'body'], axis=1, inplace=True)

    # make predictions
    count = 1
    predictions = []
    for _, row in df.iterrows():
        prediction = search(text=row['text'], bucket_keywords=bucket_keywords)
        predictions.append(prediction)
        count += 1
        if count % 100 == 0:
            logging.info(
                "processed: {}/{} articles".format(count, df.shape[0]))

    df['predictions'] = predictions

    for bucket in bucket_keywords.keys():
        df[bucket] = df['predictions'].apply(lambda x: x[bucket])

    df.drop('predictions', axis=1, inplace=True)

    if df.shape[0] != 0:
        insert_bucket_scores(df, bucket_ids, model_uuid)
        insert_entity_scores(df, bucket_ids, model_uuid)
    else:
        logging.info(f'no entities to score')
