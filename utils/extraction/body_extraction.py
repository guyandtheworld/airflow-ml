import requests
import urllib3

from dragnet import extract_content
from threading import Thread

from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def gen_text_dragnet(*articles):
    for article in articles:
        try:
            r = requests.get(article.url, verify=False)
            content = extract_content(r.content)
        except ConnectionError as e:
            print(e)
            content = None
        except Exception as e:
            print(e)
            content = None

        if content:
            print(content[:100])
        article.update(description=content)


def extract_body():
    global_init()
    try:
        articles = Article.objects.filter(
            description__exists=False)[:300]
    except Exception as e:
        print(e)
        raise

    for i in range(0, len(articles), 100):
        print("Batch: " + str(i // 20 + 1))
        threads = []

        for slide in range(i, i+100, 10):
            # create 2 threads with 10 requests each
            process = Thread(target=gen_text_dragnet,
                             args=articles[slide:slide+10])
            process.start()
            threads.append(process)

        for process in threads:
            process.join()
