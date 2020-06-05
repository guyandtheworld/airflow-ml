import os
import logging
import tensorflow_hub as hub
import tarfile
import pandas as pd
import numpy as np
import uuid
import psycopg2
import requests

from sklearn.metrics.pairwise import cosine_similarity
from pandas.io import sql
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from utils.data.postgres_utils import insert_values


DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]


con_str = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
connection = create_engine(con_str)

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

logging.basicConfig(level=logging.INFO)

path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")


# if the model doesn't exist, download model
if not os.path.exists(HELPER_DIRECTORY):
    os.makedirs(HELPER_DIRECTORY)

model_path = "{}/{}".format(HELPER_DIRECTORY, "4/")
embed = None


def download_and_extract_model():
    """
    Download model and extract it.
    """
    if not os.path.exists(model_path):
        logging.info("downloading the model")
        url = ("https://storage.googleapis.com/tfhub-modules/"
               "google/universal-sentence-encoder/4.tar.gz")

        target_path = "{}/{}".format(HELPER_DIRECTORY, "4.tar.gz")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as f:
                f.write(response.raw.read())

        tar = tarfile.open(target_path, "r:gz")
        tar.extractall(path=model_path)
        tar.close()
        os.remove(target_path)


def get_stories(start, end, stories_start, stories_end):
    """
    Get stories going back 10 days from the scrape date
    """

    logging.info('Getting stories between {} and {}'.format(start, end))

    query_existing = """
                    select story.uuid as "storyID_id", story.title,
                    cluster."clusterID_id",
                    story.published_date from apis_story as story
                    inner join ml_clustermap as cluster
                    on cluster."storyID_id" = story.uuid
                    where story.published_date > '{}'
                    and story.published_date <= '{}'
                    """.format(start, end)

    df = pd.read_sql(query_existing, connection).astype(
        str).set_index('storyID_id')

    query_new = """
                select story.uuid as "storyID_id", story.title,
                story.published_date
                from apis_story story where uuid not in
                (select distinct "storyID_id" from ml_clustermap mc)
                and story.published_date >= '{}' and
                story.published_date < '{}'
                """.format(stories_start, stories_end)

    print(query_new)

    df_newstories = pd.read_sql(query_new, connection).astype(
        str).set_index('storyID_id')

    df_newstories['clusterID_id'] = -1

    logging.info(
        '{} new stories to be clustered on {} previous stories'.format(
            len(df_newstories), len(df)))

    df = df.append(df_newstories, sort=True)
    return df


def cluster_stories(df):
    """
    Cluster stories based on their cosine similarities
    """

    dic_cluster = df['clusterID_id'].to_dict()
    df_stories = df[df['clusterID_id'] == -1]
    new_clusters = []
    logging.info('Creating vectors for {} stories'.format(len(df)))

    df_vectors = pd.DataFrame(embed(df.title).numpy(), index=df.index)

    logging.info('Clustering...')

    for story in df_stories.iterrows():

        df_check = pd.DataFrame(df_vectors.loc[story[0]]).T

        similarity = pd.DataFrame(
            cosine_similarity(
                df_check,
                df_vectors),
            columns=df_vectors.index,
            index=['similarity']).T

        similar = similarity[similarity['similarity'] > 0.72].sort_values(
            by='similarity', ascending=False).drop(story[0])

        if len(similar) > 0:

            sim_index = similar.iloc[0].name

            if dic_cluster[sim_index] == -1 and dic_cluster[story[0]] == -1:

                cluster = str(uuid.uuid4())
                dic_cluster[sim_index] = cluster
                dic_cluster[story[0]] = cluster
                new_clusters.append(cluster)
            elif dic_cluster[sim_index] == -1 and dic_cluster[story[0]] != -1:

                dic_cluster[sim_index] = dic_cluster[story[0]]
            else:

                dic_cluster[story[0]] = dic_cluster[sim_index]
        else:
            cluster = str(uuid.uuid4())
            new_clusters.append(cluster)
            dic_cluster[story[0]] = cluster

    return pd.Series(dic_cluster, name='clusterID_id'), new_clusters


def get_max_cluster_num():
    """
    Get highest current cluster number
    """

    max_num = pd.read_sql(
        'select max("cluster") from ml_cluster',
        connection).values[0][0]
    return max_num


def insert_cluster(new_clusters):
    """
    Inserting with replacement
    """

    logging.info('Inserting {} new clusters'.format(len(new_clusters)))

    cluster_num = get_max_cluster_num()
    cluster_ids = []

    for uuids in list(np.unique(new_clusters)):
        cluster_num += 1
        cluster_ids.append(tuple([uuids, str(cluster_num)]))

    query = """
            INSERT INTO ml_cluster (uuid,cluster) VALUES(%s,%s)
            """

    insert_values(query, cluster_ids)


def insert_clustermap(CLUSTER_MAP):
    """
    Inserting Clustermaps
    """

    logging.info('Inserting {} cluster mappings'.format(len(CLUSTER_MAP)))

    query = """
            INSERT INTO ml_clustermap (uuid,"clusterID_id","storyID_id",
            published_date) VALUES (%s,%s,%s,%s) ON CONFLICT(uuid)
            DO UPDATE SET "clusterID_id" = EXCLUDED."clusterID_id"
            """

    insert_values(query, CLUSTER_MAP)


def clustering(stories_date, stories_date_1):
    """
    Main clusterting function, which returns clusters on a specific date
    """
    global embed

    download_and_extract_model()
    embed = hub.load(model_path)

    END_DATE = stories_date_1
    DELTA = timedelta(days=10)
    START_DATE = END_DATE - DELTA
    start = str(START_DATE)
    end = str(END_DATE)

    stories_start = str(stories_date_1)
    stories_end = str(stories_date)

    df = get_stories(start, end, stories_start, stories_end)
    new = df[df['clusterID_id'] == -1].index

    if len(new) > 0:
        result, new_clusters = cluster_stories(df)
        df.update(result)

        df_new = df.loc[new]

        df_new = df_new.reset_index()
        df_new['uuid'] = [str(uuid.uuid4()) for row in df_new.iterrows()]

        CLUSTER_MAP = [tuple(row) for row in df_new[[
            'uuid',
            'clusterID_id',
            'storyID_id', 'published_date']].itertuples(index=False)]

        insert_cluster(new_clusters)
        insert_clustermap(CLUSTER_MAP)


def daily():
    """
    Daily Clustering
    """
    stories_date = datetime.now()
    stories_date_1 = stories_date - timedelta(days=1)
    clustering(stories_date, stories_date_1)


def backfill(**context):
    """
    Backfill Clustering
    """
    stories_date = datetime.strptime(context['ds'], '%Y-%m-%d')
    stories_date_1 = stories_date - timedelta(days=1)
    clustering(stories_date, stories_date_1)
