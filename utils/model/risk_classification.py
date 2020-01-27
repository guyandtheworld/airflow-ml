import nltk
import os
import urllib
import urllib.request

from google.cloud import storage
from google.cloud.storage import Blob

from tensorflow.keras.layers import Dense, LSTM, SpatialDropout1D, Embedding
from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.initializers import Constant
from tensorflow.keras.models import load_model


os.chdir(os.path.dirname(__file__))
path = os.getcwd()

HELPER_DIRECTORY = "{}/{}".format(path, "helpers")
BUCKET = "production_models"
MODEL = "risk_classification_model"


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

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "risk_model_v1.h5"))
    if not path:
        prefix = "{}/models".format(MODEL)
        blobs = storage_client.list_blobs(BUCKET, prefix=prefix)
        for blob in blobs:
            pass

        print("downloading the model")
        bucket = storage_client.get_bucket(BUCKET)
        blob = bucket.blob("{}/risk_model_v1.h5".format(prefix))
        blob.download_to_filename(path)
    else:
        # check if model new version exist
        print("model exists")

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
    if not path:
        # download tokenizer.pickle
        prefix = "{}".format(MODEL)
        print("downloading the tokenizer")
        bucket = storage_client.get_bucket(BUCKET)
        blob = bucket.blob("{}/tokenizer.pickle".format(prefix))
        blob.download_to_filename(path)
    else:
        print("tokenizer exists")

    path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "glove.6B.zip"))
    if not path:
        print("fetching glove model")
        urllib.request.urlretrieve(
            "http://nlp.stanford.edu/data/glove.6B.zip", path)
    else:
        print("glove model exists")


def load_data():
    """
    load_data
    """
    pass


def preprocess_data():
    """
    preprocess_data
    """
    pass


def make_prediction():
    """
    make_prediction
    """
    pass


def log_metrics():
    """
    log_metrics
    """
    pass
