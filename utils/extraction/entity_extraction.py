import en_core_web_md

from collections import defaultdict

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.body_analytics import BodyAnalytics
from data.mongo_setup import global_init

nlp = en_core_web_md.load()

# entity_types_to_save = ["PERSON", "NORP", "FACILITY", "ORG", "GPE"]

entity_types_to_save = ["PERSON", "ORG", "GPE"]


def named_entity_recognition(text: str) -> dict:
    """
    recognizes entities in the given text
    """

    document = nlp(text)
    dictionary = defaultdict(list)
    out = [(x.text, x.label_) for x in document.ents]
    for text, label in out:
        dictionary[label].append(text)
    ner = dict(dictionary)

    results = {}
    for key in ner:
        if key in entity_types_to_save:
            results[key] = list(set(ner[key]))

    return results


def extract_entities_from_headlines():
    """
    extracts entities using spaCy from the title
    of the article
    """

    global_init()

    try:
        articles = Article.objects.filter(
            title_analytics__exists=False)
    except Exception as e:
        print(e)
        raise

    count = 1
    print("extracting entities from {} articles".format(len(articles)))
    for article in articles:
        entities = named_entity_recognition(article.title)
        title_analytics = TitleAnalytics(entities=entities)
        article.title_analytics = title_analytics
        article.update(title_analytics=title_analytics)
        if not count % 100:
            print("processed: {}".format(count))
        count += 1
    print("finished")


def extract_entities_from_body():
    """
    extracts entities using spaCy from the body
    of the article
    """

    global_init()

    try:
        articles = Article.objects.filter(body__exists=True).filter(
            body_analytics__exists=False)
    except Exception as e:
        print(e)
        raise

    count = 1
    print("extracting entities from {} articles".format(len(articles)))
    for article in articles:
        entities = named_entity_recognition(article.body)
        body_analytics = BodyAnalytics(entities=entities)
        article.body_analytics = body_analytics
        article.update(body_analytics=body_analytics)
        if not count % 100:
            print("processed: {}".format(count))
        count += 1
    print("finished")
