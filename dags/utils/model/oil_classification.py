import os
import logging
import nltk
import numpy as np
import requests
import tensorflow_hub as hub
import pandas as pd
import tarfile

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords

from .utils import (get_model_details,
                    get_scenario_articles,
                    get_bucket_ids,
                    insert_bucket_scores,
                    insert_entity_scores)


os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

logging.basicConfig(level=logging.INFO)

path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")

nltk.download('stopwords')
stops = set(stopwords.words('english'))

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
        url = "https://storage.googleapis.com/tfhub-modules/google/universal-sentence-encoder/4.tar.gz"

        target_path = "{}/{}".format(HELPER_DIRECTORY, "4.tar.gz")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as f:
                f.write(response.raw.read())

        tar = tarfile.open(target_path, "r:gz")
        tar.extractall(path=model_path)
        tar.close()
        os.remove(target_path)


def semantic_search(text, keys):
    """
    See if the keys exist in the given article
    """

    for key in keys:
        find = text.lower().find(key)
        if (find != -1):
            return 1

    key_vectors = embed(keys).numpy()

    vec = CountVectorizer(ngram_range=(1, 2), stop_words=stops)
    _ = vec.fit_transform([text])
    features = vec.get_feature_names()
    vectors = embed(features).numpy()
    similarity = cosine_similarity(vectors, key_vectors)
    bool_result = int(np.any(similarity > 0.49))

    if bool_result:
        return 1
    else:
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


def oil_classification():
    """
    Classify the text to find important events in Oil Scenario
    """
    global embed

    download_and_extract_model()

    embed = hub.load(model_path)

    # fetch the latest model name from db
    results = get_model_details(scenario="Oil")

    if len(results) == 0:
        return

    bucket_ids, bucket_keywords = get_bucket_ids(scenario="Oil")

    model_uuid = results[0][0]

    articles = get_scenario_articles(
        model_uuid, scenario="Oil", body=True, article_count=5000)
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
