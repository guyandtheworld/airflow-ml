import logging
import nltk
import os
import uuid

import pandas as pd

from data.postgres_utils import connect, insert_values
from datetime import datetime
from google.cloud import storage
from tensorflow.keras.models import load_model

from .utils import make_prediction

os.chdir(os.path.dirname(__file__))
path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")

logging.basicConfig(level=logging.INFO)


MODEL_QUERY = """
    select am.uuid, bucket, storage_link from apis_modeldetail am
    left join
    apis_scenario scr
    on am."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk' and
    "version" = (
    select max("version") from apis_modeldetail am
    left join
    apis_scenario scr
    on am."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk')
    """


def load_model_utils():
    """
    download once and load when running from production
    """

    nltk.download('punkt')
    nltk.download('stopwords')

    storage_client = storage.Client()

    if not os.path.exists(HELPER_DIRECTORY):
        os.makedirs(HELPER_DIRECTORY)

    # fetch the latest model name from db

    results = connect(MODEL_QUERY)
    bucket = results[0][1]
    model_path = results[0][2]

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "risk_model_v1.h5"))
    if not path:
        logging.info("downloading the model")
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob("{}/risk_model_v1.h5".format(model_path))
        blob.download_to_filename(
            "{}/{}".format(HELPER_DIRECTORY, "risk_model_v1.h5"))
    else:
        # check if model new version exist
        logging.info("model exists")

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
    if not path:
        # download tokenizer.pickle
        logging.info("downloading the tokenizer")
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob("{}/tokenizer.pickle".format(model_path))
        blob.download_to_filename(
            "{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
    else:
        logging.info("tokenizer exists")


def load_data():
    """
    load data
    """

    # fetch articles which we haven't scored yet
    query = """
            select as2.uuid, title, published_date,
            src.uuid as sourceUUID
            from public.apis_story as2
            left join
            (SELECT distinct "storyID_id"
            FROM public.apis_bucketscore) ab
            on as2.uuid = ab."storyID_id"
            left join
            public.apis_source as src
            on src."name" = as2."domain"
            where ab."storyID_id" is null
            and src.uuid is not null
            limit 100
            """

    articles = connect(query)

    df = pd.DataFrame(articles, columns=[
                      "uuid", "title", "published_date", "sourceUUID"])
    logging.info("writing {} articles for preprocessing".format(df.shape[0]))
    df.to_csv("{}/articles.csv".format(HELPER_DIRECTORY), index=False)


def predict_scores():
    """
    loads model locally and makes prediction
    """
    # fetch latest model name from db and load it
    try:
        df = pd.read_csv("{}/articles.csv".format(HELPER_DIRECTORY))
    except FileNotFoundError as e:
        logging.info("article.csv doesn't exist: {}".format(e))
        return

    logging.info("making prediction on {} items".format(df.shape[0]))
    model = load_model("{}/risk_model_v1.h5".format(HELPER_DIRECTORY))

    df['predictions'] = df['title'].apply(lambda x: make_prediction(model, x))

    df['financial_crime'] = df['predictions'].apply(
        lambda x: x['financial_crime'])
    df['cyber_crime'] = df['predictions'].apply(lambda x: x['cyber_crime'])
    df['other'] = df['predictions'].apply(lambda x: x['other'])

    df.drop('predictions', axis=1, inplace=True)

    logging.info("writing {} articles for logging".format(df.shape[0]))
    df.to_csv("{}/results.csv".format(HELPER_DIRECTORY), index=False)
    os.remove("{}/articles.csv".format(HELPER_DIRECTORY))


def log_metrics():
    """
    * connect predictions to bucket
    * fetch UUIDs and connect to prediction
    """

    query = """
    select ab.uuid, model_label
    from apis_bucket ab
    left join
    apis_scenario scr
    on ab."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk'
    """

    results = connect(query)

    bucket_ids = {}
    for result in results:
        bucket_ids[result[1]] = result[0]

    try:
        df = pd.read_csv("{}/results.csv".format(HELPER_DIRECTORY))
    except FileNotFoundError as e:
        logging.info("results.csv doesn't exist : {}".format(e))
        return

    response = connect(MODEL_QUERY)
    model_uuid = response[0][0]

    values = []
    for _, row in df.iterrows():
        log_row = {}
        for bucket in bucket_ids.keys():
            log_row["uuid"] = str(uuid.uuid4())
            log_row["storyID_id"] = row["uuid"]
            log_row["entryTime"] = str(datetime.now())
            log_row["grossScore"] = row[bucket]
            log_row["bucketID_id"] = bucket_ids[bucket]
            log_row["modelID_id"] = model_uuid
            log_row["sourceID_id"] = row["sourceUUID"]
            log_row["storyDate"] = row["published_date"]

    insert_query = """
    INSERT INTO public.apis_bucketscore
    (uuid, "storyID_id", "entryTime", "grossScore",
    "bucketID_id", "modelID_id", "sourceID_id", "storyDate")
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
