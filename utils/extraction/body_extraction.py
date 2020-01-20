import requests
import urllib3
import time
import warnings
import concurrent.futures

import pandas as pd
from dragnet import extract_content

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init


def warn(*args, **kwargs):
    pass


out = []
CONNECTIONS = 100
TIMEOUT = 5

warnings.warn = warn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def gen_text_dragnet(article, timeout):
    r = requests.get(article.url, verify=False)
    content = extract_content(r.content)
    article.update(body=content[:500])


def extract_body():
    """
    the problem probably is that we're running out of
    memory. so we'll run this process once every 30 minutes
    and only process about 2k articles
    """
    global_init()
    try:
        articles = Article.objects.filter(
            body__exists=False)[:500]
    except Exception as e:
        print(e)
        raise

    print("extracting entities from {} articles".format(len(articles)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(gen_text_dragnet, article, TIMEOUT)
                         for article in articles)
        time1 = time.time()
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
            except Exception as exc:
                data = str(type(exc))
            finally:
                out.append(data)
                print(str(len(out)), end="\r")

        time2 = time.time()

    print(f'Took {time2-time1:.2f} s')
    print(pd.Series(out).value_counts())
