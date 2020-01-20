import concurrent.futures
import requests
import time
import urllib3
import warnings

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


def do_request(url):
    try:
        requests.head(url, timeout=5)
    except Exception:
        return "", 404

    try:
        res = requests.get(url, verify=False, timeout=5)
        content = extract_content(res.content)
        return content, res.status_code
    except Exception:
        return "", 404


def gen_text_dragnet(article, timeout):
    content, status_code = do_request(article.url)
    article.update(body=content[:500], status_code=status_code)
    return status_code


def extract_body():
    """
    Processing broken URLs are a huge pain in the ass
    """
    global_init()
    try:
        articles = Article.objects.filter(
            status_code__exists=False)[:5000]
    except Exception as e:
        print(e)
        raise

    print("extracting bodies from {} articles".format(len(articles)))

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
