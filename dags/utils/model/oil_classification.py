import os
import logging
import nltk
import numpy as np
import requests
import tensorflow_hub as hub
import tensorflow as tf

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords

from .utils import get_model_details


logging.basicConfig(level=logging.INFO)

os.chdir(os.path.dirname(__file__))
path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")

nltk.download('stopwords')
stops = set(stopwords.words('english'))

# enlarge to match more keywords semantically
keys = ['shutdown', "explosion", "blast"]


# if the model doesn't exist, download model
if not os.path.exists(HELPER_DIRECTORY):
    os.makedirs(HELPER_DIRECTORY)

path = "{}/{}".format(HELPER_DIRECTORY, "4/saved_model.pb")
# file_exists = os.path.isfile(path)
# if not file_exists:
#     logging.info("downloading the model")
#     url = "https://tfhub.dev/google/universal-sentence-encoder/4"

#     r = requests.get(url)
#     with open(path, 'wb') as f:
#         f.write(r.content)

# print(path)
embed = hub.load(path)


def semantic_search(text, keys=keys):
    """
    See if the keys exist in the given article
    """
    for key in keys:

        find = text.find(key)
        if (find != -1):
            return 1

    key_vectors = embed(keys).numpy()

    vec = CountVectorizer(ngram_range=(1, 2), stop_words=stops)
    x = vec.fit_transform(text)
    features = vec.get_feature_names()
    vectors = embed(features).numpy()
    similarity = cosine_similarity(vectors, key_vectors)

    return int(np.any(similarity > 0.49))


def oil_classification():
    """
    Classify the text to find important events in Oil Scenario
    """

    # fetch the latest model name from db
    results = get_model_details("Oil")

    print(results)
    if len(results) == 0:
        return

    # model_uuid = results[0][0]
    # bucket = results[0][1]
    # model_path = results[0][2]
    # model_name = results[0][3]
