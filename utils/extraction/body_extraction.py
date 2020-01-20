import requests
import urllib3
import warnings

from dragnet import extract_content
from threading import Thread
from multiprocessing import Pool

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init


def warn(*args, **kwargs):
    pass


warnings.warn = warn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def gen_text_dragnet(article):
    print(article.url)
    try:
        r = requests.get(article.url, verify=False)
        content = extract_content(r.content)
    except ConnectionError as e:
        print(e)
        content = ""
    except Exception as e:
        print(e)
        content = ""
    # save only this much or we run out of mongo space
    article.update(body=content[:500])


def extract_body():
    global_init()
    try:
        articles = Article.objects.filter(
            body__exists=False)[:1000]
    except Exception as e:
        print(e)
        raise
    print("extracting entities from {} articles".format(len(articles)))

    p = Pool(20)
    p.map(gen_text_dragnet, articles)
    p.terminate()
    p.join()
