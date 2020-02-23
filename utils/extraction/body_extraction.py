import concurrent.futures
import re
import logging
import requests
import time
import urllib3
import warnings

import pandas as pd
from dragnet import extract_content

from data.postgres_utils import connect, update_values

logging.basicConfig(level=logging.INFO)


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
    content, status_code = do_request(article[1])
    body = body_cleaning(content[:600])
    return (article[0], body, status_code)


def extract_body():
    """
    Processing broken URLs are a huge pain in the ass
    """

    query = """
               SELECT uuid, url
               FROM public.apis_story
               WHERE status_code IS NULL
               LIMIT 100;
            """

    response = connect(query)

    logging.info("extracting bodies from {} articles".format(len(response)))

    values = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(gen_text_dragnet, article, TIMEOUT)
                         for article in response)
        time1 = time.time()
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                uuid, body, status = future.result()
                values.append((uuid, body, status))
            except Exception as exc:
                status = str(type(exc))
            finally:
                out.append(status)
                print(str(len(out)), end="\r")

        time2 = time.time()

    query = """
            UPDATE public.apis_story AS t
            SET body = e.body::text, status_code = e.status_code
            FROM (VALUES %s) AS e(uuid, body, status_code)
            WHERE e.uuid = t.uuid::text;
            """

    update_values(query, values)

    logging.info(f'Took {time2-time1:.2f} s')
    logging.info(pd.Series(out).value_counts())
