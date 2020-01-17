import en_core_web_sm

from collections import defaultdict

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init

nlp = en_core_web_sm.load()


def named_entity_recognition(text: str) -> dict:
    """
    recognizes entities in the given text
    """

    document = nlp(text)
    dictionary = defaultdict(list)
    out = [(x.text, x.label_) for x in document.ents]
    for text, label in out:
        dictionary[label].append(text)
    return dict(dictionary)


def extract_entities(**params):
    # setup connection to database
    global_init()
    try:
        articles = Article.objects.filter(
            title_analytics__exists=False)[:300]
    except Exception as e:
        print(e)
        raise

    count = 0
    print("extracting entities from {} articles".format(len(articles)))
    for article in articles:
        entities = named_entity_recognition(article.title)
        title_analytics = TitleAnalytics(entities=entities)
        article.title_analytics = title_analytics
        status = article.update(title_analytics=title_analytics)
        if not status:
            print("{} failed".format(article._id))
        if not count % 100:
            print("processed: {}".format(count//100))
        count += 1
    print("finished")
