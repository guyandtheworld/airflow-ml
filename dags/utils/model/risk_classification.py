import logging
import nltk
import os
import time
import shutil

import pandas as pd

from google.cloud import storage
from tensorflow.keras.models import load_model

from .utils import (make_prediction,
                    get_model_details,
                    get_scenario_articles,
                    get_bucket_ids,
                    insert_bucket_scores,
                    insert_entity_scores)

os.chdir(os.path.dirname(__file__))
path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")

logging.basicConfig(level=logging.INFO)


def risk_classification():
    """
    download once and load when running from production
    """

    nltk.download('punkt')
    nltk.download('stopwords')

    storage_client = storage.Client()

    if not os.path.exists(HELPER_DIRECTORY):
        os.makedirs(HELPER_DIRECTORY)

    # fetch the latest model name from db
    results = get_model_details()

    if len(results) == 0:
        return

    model_uuid = results[0][0]
    bucket = results[0][1]
    model_path = results[0][2]
    model_name = results[0][3]

    logging.info("{}/{}".format(model_path, model_name))
    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, model_name))
    if not path:
        shutil.rmtree("{}/".format(HELPER_DIRECTORY))
        os.makedirs(HELPER_DIRECTORY)
        logging.info("downloading the model")
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob("{}/{}".format(model_path, model_name))
        blob.download_to_filename(
            "{}/{}".format(HELPER_DIRECTORY, model_name))

        # download tokenizer.pickle
        logging.info("downloading the tokenizer")
        blob = bucket.blob("{}/tokenizer.pickle".format(model_path))
        blob.download_to_filename(
            "{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
    else:
        logging.info("model and tokenizer exists")

    # fetch articles
    articles = get_scenario_articles(model_uuid)

    df = pd.DataFrame(articles, columns=[
                      "uuid", "title", "published_date",
                      "sourceUUID", "entityUUID"])

    # loads model locally and makes prediction
    # fetch latest model name from db and load it
    time1 = time.time()

    logging.info("making prediction on {} items".format(df.shape[0]))
    model = load_model("{}/{}".format(HELPER_DIRECTORY, model_name))

    # make predictions
    count = 1
    predictions = []
    for _, row in df.iterrows():
        prediction = make_prediction(model, row['title'])
        predictions.append(prediction)
        count += 1
        if count % 100 == 0:
            logging.info(
                "processed: {}/{} articles".format(count, df.shape[0]))

    df['predictions'] = predictions

    df['financial_crime'] = df['predictions'].apply(
        lambda x: x['financial_crime'])
    df['cyber_crime'] = df['predictions'].apply(lambda x: x['cyber_crime'])
    df['other'] = df['predictions'].apply(lambda x: x['other'])

    df.drop('predictions', axis=1, inplace=True)

    time2 = time.time()
    logging.info(f'Took {time2-time1:.2f} s')

    bucket_ids = get_bucket_ids()

    if df.shape[0] != 0:
        insert_bucket_scores(df, bucket_ids, model_uuid)
        insert_entity_scores(df, bucket_ids, model_uuid)
    else:
        logging.info(f'no entities to score')
