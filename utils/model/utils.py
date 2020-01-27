import os
import pickle

from google.cloud import storage
from google.cloud.storage import Blob

from tensorflow.keras.preprocessing.sequence import pad_sequences


MAX_LEN = 100
BUCKET = "production_models"


def padding(corpus, train=True):
    with open('tokenizer.pickle', 'rb') as tok:
        tokenizer = pickle.load(tok)

    sequences = tokenizer.texts_to_sequences(corpus)
    news_pad = pad_sequences(sequences, maxlen=MAX_LEN)
    word_index = None
    return news_pad, word_index


def upload_ml_stuff_to_bucket(blob_name, path_to_file):
    """
    upload the model and the helper libraries into
    S3 with proper versioning
    """
    print("uploading - {}".format(path_to_file))
    print("uploading to - {}".format(blob_name))

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


upload_version_model()
