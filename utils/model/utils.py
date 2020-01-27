import pickle

from tensorflow.keras.preprocessing.sequence import pad_sequences


MAX_LEN = 100


def padding(corpus, train=True):
    with open('tokenizer.pickle', 'rb') as tok:
        tokenizer = pickle.load(tok)

    sequences = tokenizer.texts_to_sequences(corpus)
    news_pad = pad_sequences(sequences, maxlen=MAX_LEN)
    word_index = None
    return news_pad, word_index
