import nltk

from tensorflow.keras.layers import Dense, LSTM, SpatialDropout1D, Embedding
from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.initializers import Constant
from tensorflow.keras.models import load_model


def load_model_helpers():
    """
    download once and load when running from production
    """
    # model = load_model("/content/risk_model_v1.h5")
    nltk.download('punkt')
    nltk.download('stopwords')
    # download http://nlp.stanford.edu/data/glove.6B.zip
    # download tokenizer.pickle


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
