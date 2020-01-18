import requests
import urllib3
import warnings

from dragnet import extract_content
from threading import Thread

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init


def warn(*args, **kwargs):
    pass


warnings.warn = warn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def gen_text_dragnet(*articles):
    for article in articles:
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
            body__exists=False)
    except Exception as e:
        print(e)
        raise

    for i in range(0, len(articles), 100):
        print("processed: {}".format(i))
        threads = []

        for slide in range(i, i+100, 10):
            # create 2 threads with 10 requests each
            process = Thread(target=gen_text_dragnet,
                             args=articles[slide:slide+10])
            process.start()
            threads.append(process)

        for process in threads:
            process.join()
