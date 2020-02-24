import logging
import nltk
import os
import urllib
import urllib.request

import pandas as pd

from data.postgres_utils import connect, insert_values
from google.cloud import storage
from tensorflow.keras.models import load_model

from .utils import make_prediction

os.chdir(os.path.dirname(__file__))
path = os.getcwd()

logging.basicConfig(level=logging.INFO)


HELPER_DIRECTORY = "{}/{}".format(path, "helpers")
BUCKET = "production_models"
MODEL = "risk_classification_model"

BUCKET_UUID = {
    "financial_risk": "2fa858cf-f8c3-4fe8-bd02-ae66aae2b909",
    "cyber_crime": "126c6022-96a0-4769-a607-905cec0a7d46",
    "other": "ee3406eb-7302-46c0-a60f-80f3fc603cda"}


def load_model_utils():
    """
    download once and load when running from production
    """

    query = """
    select am.uuid, storage_link from apis_modeldetail am
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

    nltk.download('punkt')
    nltk.download('stopwords')

    storage_client = storage.Client()

    if not os.path.exists(HELPER_DIRECTORY):
        os.makedirs(HELPER_DIRECTORY)

    # fetch the latest model name from db

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "risk_model_v1.h5"))
    if not path:
        prefix = "{}/models".format(MODEL)
        blobs = storage_client.list_blobs(BUCKET, prefix=prefix)
        for blob in blobs:
            pass

        logging.info("downloading the model")
        bucket = storage_client.get_bucket(BUCKET)
        blob = bucket.blob("{}/risk_model_v1.h5".format(prefix))
        blob.download_to_filename(path)
    else:
        # check if model new version exist
        logging.info("model exists")

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
    if not path:
        # download tokenizer.pickle
        prefix = "{}".format(MODEL)
        logging.info("downloading the tokenizer")
        bucket = storage_client.get_bucket(BUCKET)
        blob = bucket.blob("{}/tokenizer.pickle".format(prefix))
        blob.download_to_filename(path)
    else:
        logging.info("tokenizer exists")

    # path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "glove.6B.zip"))
    # if not path:
    #     logging.info("fetching glove model")
    #     urllib.request.urlretrieve(
    #         "http://nlp.stanford.edu/data/glove.6B.zip", path)
    # else:
    #     logging.info("glove model exists")


def load_data():
    """
    load data
    """

    # fetch articles which we haven't scored yet
    query = """
            select as2.uuid, title, published_date, src.uuid as sourceUUID
            from public.apis_story as2
            left join
            (SELECT distinct "storyID_id"
            FROM public.apis_bucketscore) ab
            on as2.uuid = ab."storyID_id"
            left join
            public.apis_source as src
            on src."name" = as2."domain"
            where ab."storyID_id" is null
            """

    articles = connect(query)

    df = pd.DataFrame(articles, columns=[
                      "uuid", "title", "published_date", "domain"])
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
        logging.info("article.csv doesn't exist")
        return

    logging.info("making prediction on {} items".format(df.shape[0]))
    model = load_model("{}/risk_model_v1.h5".format(HELPER_DIRECTORY))

    df['predictions'] = df['title'].apply(lambda x: make_prediction(model, x))
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

    try:
        df = pd.read_csv("{}/results.csv".format(HELPER_DIRECTORY))
    except FileNotFoundError as e:
        logging.info("results.csv doesn't exist")
        return
