import hashlib
import json
import os

from datetime import datetime

from .article import Article


def gdelt(data, metadata):
    """
    first item on the list is the common name
    second item is the ticker. should be updated
    when aliases are added
    """

    date_format = "%Y%m%dT%H%M%SZ"

    articles = []
    for key in data.keys():
        for article in data[key]:
            hexdigest = hashlib.md5(article['url'].encode()).hexdigest()
            normalized_d = datetime.strptime(article["seendate"], date_format)
            article = Article(
                entity_id=metadata["entity_object"].id,
                title=article["title"],
                unique_hash=hexdigest,
                url=article["url"],
                search_keyword=key,
                published_date=normalized_d,
                internal_source="gdelt",
                domain=article["domain"],
                language=article["language"].lower(),
                source_country=article["sourcecountry"].lower(),
                raw_file_source=metadata["source_file"]
            )
            articles.append(article)
    return articles


def google_news(data, metadata):
    """
    format for converting google news data
    into our model
    """
    cwd = os.path.dirname(__file__)
    with open("{}/codes.json".format(cwd), "r") as r:
        codes = json.load(r)

    date_format = "%a, %d %b %Y %H:%M:%S %Z"
    articles = []
    for key in data.keys():
        for region in data[key]:

            # defines the language and the country
            # associated with each request
            # params = region["search_params"]
            # source_country = codes["country"][params["gl"]]
            # language = codes["language"][params["gl"]]
            for article in region["item"]:
                # generating a unique hash for our article url
                hexdigest = hashlib.md5(article['link'].encode()).hexdigest()
                normalized_d = datetime.strptime(
                    article["pubDate"], date_format)

                article = Article(
                    entity_id=metadata["entity_object"].id,
                    title=article["title"],
                    unique_hash=hexdigest,
                    url=article["link"],
                    search_keyword=key,
                    published_date=normalized_d,
                    internal_source="google_news",
                    domain=article["source"]["@url"],
                    description=article["description"],
                    language="default",
                    source_country="default",
                    raw_file_source=metadata["source_file"]
                )
                articles.append(article)
    return articles
