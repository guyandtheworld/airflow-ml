import os
import sys
import json
import random
from pathlib import Path

path = Path(os.path.abspath(os.path.dirname(__file__)))  # noqa
sys.path.insert(0, "{}/indexing_stages".format(path.parent))  # noqa

from data.article import Article
from data.mongo_setup import global_init


global_init()

objects = Article.objects().filter(internal_source="google_news")

objects = random.sample(list(objects), 1000)


all_records = []
for obj in objects:
    all_records.append(json.loads(obj.to_json()))

with open('samples.json', 'w') as fp:
    json.dump(all_records, fp)
