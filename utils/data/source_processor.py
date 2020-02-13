import hashlib

from datetime import datetime

from .article import Article


def gdelt(data, entity_id, source_file):
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
                entity_id=entity_id,
                title=article["title"],
                unique_hash=hexdigest,
                url=article["url"],
                search_keyword=key,
                published_date=normalized_d,
                internal_source="gdelt",
                domain=article["domain"],
                language=article["language"].lower(),
                source_country=article["sourcecountry"].lower(),
                raw_file_source=source_file
            )
            articles.append(article)
    return articles


def google_news(data, entity_id, source_file):
    """
    format for converting google news data
    into our model
    """
    date_format = "%Y-%m-%d %H:%M:%S"
    articles = []
    for key in data.keys():
        for article in data[key]:
            hexdigest = hashlib.md5(article['url'].encode()).hexdigest()
            normalized_d = datetime.strptime(
                article["pubDate"], date_format)

            article = Article(
                entity_id=entity_id,
                title=article["title"],
                unique_hash=hexdigest,
                url=article["url"],
                search_keyword=key,
                published_date=normalized_d,
                internal_source="google_news",
                domain=article["source"],
                description=article["description"],
                language=article["language"],
                source_country=article["country"],
                raw_file_source=source_file
            )
            articles.append(article)
    return articles
