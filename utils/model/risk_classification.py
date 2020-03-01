import logging
import nltk
import os
import time
import uuid
import shutil

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
    select am.uuid, bucket, storage_link, am."name" from apis_modeldetail am
    left join
    apis_scenario scr on am."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk' and
    "version" = (select max("version") from apis_modeldetail am
    left join
    apis_scenario scr on am."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk')
    """


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

    results = connect(MODEL_QUERY)

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

    # fetch articles which we haven't scored
    # using our current model yet
    query = """
            select as2.uuid, title, published_date, src.uuid as sourceUUID,
            "entityID_id" as entityUUID from public.apis_story as2
            left join
            (SELECT distinct "storyID_id" FROM public.apis_bucketscore
            where "modelID_id" = '{}') ab on as2.uuid = ab."storyID_id"
            left join
            public.apis_source as src on src."name" = as2."domain"
            where ab."storyID_id" is null and src.uuid is not null
            limit 10000
            """.format(model_uuid)

    articles = connect(query)

    df = pd.DataFrame(articles, columns=[
                      "uuid", "title", "published_date",
                      "sourceUUID", "entityUUID"])

    # loads model locally and makes prediction
    # fetch latest model name from db and load it
    time1 = time.time()

    logging.info("making prediction on {} items".format(df.shape[0]))
    model = load_model("{}/{}".format(HELPER_DIRECTORY, model_name))

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

    # connect predictions to bucket
    # fetch UUIDs and connect to prediction

    query = """
    select ab.uuid, model_label from apis_bucket ab
    left join apis_scenario scr on ab."scenarioID_id" = scr.uuid
    where scr."name" = 'Risk'
    """

    results = connect(query)

    bucket_ids = {}
    for result in results:
        bucket_ids[result[1]] = result[0]

    values = []
    for _, row in df.iterrows():
        for bucket in bucket_ids.keys():
            log_row = (str(uuid.uuid4()),
                       row["uuid"],
                       str(datetime.now()),
                       row[bucket],
                       bucket_ids[bucket],
                       model_uuid,
                       row["sourceUUID"],
                       row["published_date"])
            values.append(log_row)

    logging.info("writing {} articles into bucket scores".format(df.shape[0]))
    insert_query = """
    INSERT INTO public.apis_bucketscore
    (uuid, "storyID_id", "entryTime", "grossScore",
    "bucketID_id", "modelID_id", "sourceID_id", "storyDate")
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
    """
    insert_values(insert_query, values)

    # scoring all the entities
    logging.info("writing {} articles into entity scores".format(df.shape[0]))
    values = []
    for _, row in df.iterrows():
        for bucket in bucket_ids.keys():
            log_row = (str(uuid.uuid4()),
                       row["uuid"],
                       row[bucket],
                       bucket_ids[bucket],
                       row["entityUUID"],
                       model_uuid,
                       row["sourceUUID"],
                       str(datetime.now())
                       )
            values.append(log_row)

    insert_query = """
        INSERT INTO public.apis_entityscore
        (uuid, "storyID_id", "grossScore", "bucketID_id",
        "entityID_id", "modelID_id", "sourceID_id", "entryTime")
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
    """

    insert_values(insert_query, values)
