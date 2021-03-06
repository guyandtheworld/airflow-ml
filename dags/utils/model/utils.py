import logging
import os
import pickle
import uuid
import pandas as pd

from datetime import datetime
from google.cloud import storage
from google.cloud.storage import Blob
from tensorflow.keras.preprocessing.sequence import pad_sequences

from utils.data.postgres_utils import connect, insert_values


logging.basicConfig(level=logging.INFO)

BUCKET = "production_models"


def padding(corpus, train=True):
    os.chdir(os.path.dirname(__file__))
    path = os.getcwd()
    HELPER_DIRECTORY = "{}/{}".format(path, "helpers")
    path = "{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle")
    with open(path, 'rb') as tok:
        tokenizer = pickle.load(tok)
    sequences = tokenizer.texts_to_sequences(corpus)
    news_pad = pad_sequences(sequences, maxlen=100)
    word_index = None
    return news_pad, word_index


def upload_ml_stuff_to_bucket(blob_name, path_to_file):
    """
    upload the model and the helper libraries into
    S3 with proper versioning
    """
    logging.info("uploading - {}".format(path_to_file))
    logging.info("uploading to - {}".format(blob_name))

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)

    # path = "".format()

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url


def upload_version_model(model=False):
    resource = "tokenizer.pickle"
    if model:
        blob_name = "risk_classification_model/models/{}".format(resource)
    else:
        blob_name = "risk_classification_model/{}".format(resource)

    os.chdir(os.path.dirname(__file__))
    path = os.getcwd()

    path = "{}/helpers/{}".format(path, resource)
    upload_ml_stuff_to_bucket(blob_name, path)


def make_boolean(results: dict) -> dict:
    """
    convert probability to boolean
    """
    bool_res = {}
    maximum = max(results, key=results.get)
    for k in results.keys():
        if k == maximum:
            bool_res[k] = results[k]
        else:
            bool_res[k] = 0
    return bool_res


def make_prediction(model, test: str):
    """
    make predictions using the model
    """
    if isinstance(test, str):
        test = padding(list([test]), False)[0]
        y_pre = model.predict(test)[0]

        result_dict = {'financial_crime': y_pre[0],
                       'cyber_crime': y_pre[1], 'other': y_pre[2]}
        return result_dict
    else:
        return {'financial_crime': 0,
                'cyber_crime': 0, 'other': 1}


def get_model_details(scenario):
    """
    get the bucket and model details
    """

    model_query = """
        select am.uuid, bucket, storage_link, am."name" from apis_modeldetail am
        left join
        apis_scenario scr on am."scenarioID_id" = scr.uuid
        where scr."name" = '{}' and
        "version" = (select max("version") from apis_modeldetail am
        left join
        apis_scenario scr on am."scenarioID_id" = scr.uuid
        where scr."name" = '{}')
        """.format(scenario, scenario)

    results = connect(model_query)
    logging.info(results)
    return results


def get_scenario_articles(model_uuid, scenario, body=False, article_count=5000):
    """
    Fetch articles which we haven't scored
    using our current model yet which belongs
    to our risk scenario

    Articles filtered based on the following criteria:
    * No score in bucketscores
    * Source exists
    * No relations in entitymap, hence entity extraction not done
    * EntityID in entityref
    """

    if body:
        query = """
                select story.uuid, title, body, published_date, src.uuid as sourceUUID,
                "entityID_id" as entityUUID from public.apis_story story
                left join
                (SELECT distinct "storyID_id" FROM public.apis_bucketscore
                where "modelID_id" = '{}') ab on story.uuid = ab."storyID_id"
                inner join (select "storyID_id", (array_agg(body))[1] as body
                from apis_storybody group by "storyID_id") story_body
                on story_body."storyID_id" = story.uuid
                inner join
                (select distinct "storyID_id" from apis_storyentitymap) entitymap
                on story.uuid = entitymap."storyID_id"
                inner join
                public.apis_source as src on src."name" = story."domain"
                left join
                public.apis_scenario as scnr on scnr.uuid = story."scenarioID_id"
                where scnr."name" = '{}'
                and ab."storyID_id" is null
                and "entityID_id" in (select uuid from apis_storyentityref story)
                limit {}
                """.format(model_uuid, scenario, article_count)
    else:
        query = """
                select story.uuid, title, published_date, src.uuid as sourceUUID,
                "entityID_id" as entityUUID from public.apis_story story
                left join
                (SELECT distinct "storyID_id" FROM public.apis_bucketscore
                where "modelID_id" = '{}') ab on story.uuid = ab."storyID_id"
                inner join
                (select distinct "storyID_id" from apis_storyentitymap) entitymap
                on story.uuid = entitymap."storyID_id"
                inner join
                public.apis_source as src on src."name" = story."domain"
                left join
                public.apis_scenario as scnr on scnr.uuid = story."scenarioID_id"
                where scnr."name" = '{}'
                and ab."storyID_id" is null
                and "entityID_id" in (select uuid from apis_storyentityref story)
                limit {}
                """.format(model_uuid, scenario, article_count)

    articles = connect(query)
    return articles


def get_bucket_ids(scenario):
    """
    Connect predictions to bucket
    fetch UUIDs and connect to prediction
    """

    query = """
    select ab.uuid, model_label, keywords from apis_bucket ab
    left join apis_scenario scr on ab."scenarioID_id" = scr.uuid
    where scr."name" = '{}'
    """.format(scenario)

    results = connect(query)

    bucket_ids = {}
    for result in results:
        bucket_ids[result[1]] = result[0]

    bucket_keywords = {}
    for result in results:
        bucket_keywords[result[1]] = result[2]

    return bucket_ids, bucket_keywords


def insert_bucket_scores(df, bucket_ids, model_uuid):
    """
    Insert bucket scores into the db
    """
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


def add_article_entity_to_score(row):
    """
    check if the api_entity is present in the
    detected entities list
    """
    if pd.isna(row["entity_id"]):
        return [row["entityUUID"]]
    else:
        entities = list(row["entity_id"])
        if row["entityUUID"] not in entities:
            entities.append(row["entityUUID"])
            return entities
        return entities


def insert_entity_scores(df, bucket_ids, model_uuid):
    """
    Insert entity scores into the db by fetching the uuid
    of all the entities present in a particular article
    """

    article_uuids = df["uuid"].unique()

    ids_str = "', '".join(article_uuids)
    ids_str = "('{}')".format(ids_str)

    query = """
    select "entityID_id", "storyID_id" from apis_storyentitymap as2
    where as2."storyID_id" in {}
    """.format(ids_str)

    results = connect(query, verbose=False)

    entity_df = pd.DataFrame(results, columns=[
        "entity_id", "story_id"])

    logging.info("{} articles found".format(entity_df["story_id"].nunique()))
    logging.info("{} entities found".format(entity_df["entity_id"].nunique()))

    # get all unique entities in the articles
    entity_df = entity_df.groupby(["story_id"])[
        "entity_id"].apply(set).reset_index()

    df = df.merge(entity_df, how="left", left_on="uuid", right_on="story_id")
    df.drop("story_id", axis=1, inplace=True)

    df["entity_id"] = df.apply(add_article_entity_to_score, axis=1)

    values = []

    """
    for each row
    for each entities
    and for each bucket
    insert the scores
    """
    for _, row in df.iterrows():
        for entity in row["entity_id"]:
            for bucket in bucket_ids.keys():
                log_row = (str(uuid.uuid4()),
                           row["uuid"],
                           row[bucket],
                           bucket_ids[bucket],
                           entity,
                           model_uuid,
                           row["sourceUUID"],
                           str(datetime.now())
                           )
                values.append(log_row)

    logging.info("writing {} articles into entity scores".format(df.shape[0]))
    insert_query = """
        INSERT INTO public.apis_entityscore
        (uuid, "storyID_id", "grossScore", "bucketID_id",
        "entityID_id", "modelID_id", "sourceID_id", "entryTime")
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
    """

    insert_values(insert_query, values)
