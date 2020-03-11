import logging
import os
import pickle

from google.cloud import storage
from google.cloud.storage import Blob

from tensorflow.keras.preprocessing.sequence import pad_sequences


logging.basicConfig(level=logging.INFO)

BUCKET = "production_models"

os.chdir(os.path.dirname(__file__))
path = os.getcwd()
HELPER_DIRECTORY = "{}/{}".format(path, "helpers")


def padding(corpus, train=True):
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
