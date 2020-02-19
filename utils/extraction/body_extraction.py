import concurrent.futures
import re
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
CONNECTIONS = 80
TIMEOUT = 5

warnings.warn = warn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) \
           AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


def do_request(url):
    try:
        requests.head(url, verify=False, timeout=10, headers=headers)
    except Exception:
        return "", 404

    try:
        res = requests.get(url, verify=False, timeout=10, headers=headers)
        content = extract_content(res.content)
        return content, res.status_code
    except Exception:
        return "", 404


def body_cleaning(text):
    """
    function to clean news content
    """

    text = re.sub(r'\n|\xa0|\xad|\d+\.\d+\W?|\+', " ", text)
    text = re.sub(r'\'', "", text)
    text = re.sub(r'(By|by)\s\S+\s\S+', '', text)
    text = re.sub(r'(Published|Updated):\s\d\d:\d\d\s\S{3}', "", text)
    text = re.sub(r'\d{2}\s(January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}',
                  "", text)
    text = re.sub(r'FILE\sPHOTO:.*Photo', "", text)
    return text


def gen_text_dragnet(article, timeout):
    content, status_code = do_request(article["url"])
    cleaned_content = body_cleaning(content[:500])
    article.update(body=cleaned_content, status_code=status_code)
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
