# import nltk
# import os
# import urllib
# import urllib.request

# import pandas as pd

# from google.cloud import storage
# from tensorflow.keras.models import load_model

# from .utils import make_prediction

# os.chdir(os.path.dirname(__file__))
# path = os.getcwd()

# HELPER_DIRECTORY = "{}/{}".format(path, "helpers")
# BUCKET = "production_models"
# MODEL = "risk_classification_model"


# def load_model_utils():
#     """
#     download once and load when running from production
#     """
#     nltk.download('punkt')
#     nltk.download('stopwords')

#     storage_client = storage.Client()

#     if not os.path.exists(HELPER_DIRECTORY):
#         os.makedirs(HELPER_DIRECTORY)

#     # fetch the latest model name from db

#     path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "risk_model_v1.h5"))
#     if not path:
#         prefix = "{}/models".format(MODEL)
#         blobs = storage_client.list_blobs(BUCKET, prefix=prefix)
#         for blob in blobs:
#             pass

#         print("downloading the model")
#         bucket = storage_client.get_bucket(BUCKET)
#         blob = bucket.blob("{}/risk_model_v1.h5".format(prefix))
#         blob.download_to_filename(path)
#     else:
#         # check if model new version exist
#         print("model exists")

#     path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "tokenizer.pickle"))
#     if not path:
#         # download tokenizer.pickle
#         prefix = "{}".format(MODEL)
#         print("downloading the tokenizer")
#         bucket = storage_client.get_bucket(BUCKET)
#         blob = bucket.blob("{}/tokenizer.pickle".format(prefix))
#         blob.download_to_filename(path)
#     else:
#         print("tokenizer exists")

#     path = os.path.isfile("{}/{}".format(HELPER_DIRECTORY, "glove.6B.zip"))
#     if not path:
#         print("fetching glove model")
#         urllib.request.urlretrieve(
#             "http://nlp.stanford.edu/data/glove.6B.zip", path)
#     else:
#         print("glove model exists")


# def load_data():
#     """
#     load data
#     """
#     try:
#         # fetch articles which we haven't scored yet
#         pipeline = [{"$match": {"buckets": {"$ne": "null"}}},
#                     {"$limit": 50},
#                     {"$project": {
#                         "title": 1,
#                         "published_date": 1}
#                      }]

#     except Exception as e:
#         print(e)
#         return

#     # df = pd.DataFrame(list(articles))
#     # print("writing {} articles for preprocessing".format(df.shape[0]))
#     # df.to_csv("{}/articles.csv".format(HELPER_DIRECTORY), index=False)


# def predict_scores():
#     """
#     loads model locally and makes prediction
#     """
#     # fetch latest model name from db and load it
#     try:
#         df = pd.read_csv("{}/articles.csv".format(HELPER_DIRECTORY))
#     except FileNotFoundError as e:
#         print("article.csv doesn't exist")
#         return

#     print("making prediction on {} items".format(df.shape[0]))
#     model = load_model("{}/risk_model_v1.h5".format(HELPER_DIRECTORY))

#     df['predictions'] = df['title'].apply(lambda x: make_prediction(model, x))
#     print("writing {} articles for logging".format(df.shape[0]))
#     df.to_csv("{}/results.csv".format(HELPER_DIRECTORY), index=False)
#     os.remove("{}/articles.csv".format(HELPER_DIRECTORY))


# def log_metrics():
#     """
#     * connect predictions to bucket
#     * fetch UUIDs and connect to prediction
#     """
#     try:
#         df = pd.read_csv("{}/results.csv".format(HELPER_DIRECTORY))
#     except FileNotFoundError as e:
#         print("results.csv doesn't exist")
#         return
