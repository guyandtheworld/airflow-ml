import hashlib

from .article import Article


def gdelt(data, metadata):
    """
    first item on the list is the common name
    second item is the ticker. should be updated
    when aliases are added
    """
    articles = []
    for key in data.keys():
        for article in data[key]:
            hexdigest = hashlib.md5(article['url'].encode()).hexdigest()

            article = Article(
                entity_id=metadata["entity_object"].id,
                title=article["title"],
                unique_hash=hexdigest,
                url=article["url"],
                search_keyword=key,
                published_date=article["seendate"],
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
    normalize date
    """
    articles = []
    for key in data.keys():
        for article in data[key]:
            hexdigest = hashlib.md5(article['link'].encode()).hexdigest()

            article = Article(
                entity_id=metadata["entity_object"].id,
                title=article["title"],
                unique_hash=hexdigest,
                url=article["link"],
                search_keyword=key,
                published_date=article["pubDate"],
                internal_source="google_news",
                domain=article["source"]["@url"],
                description=article["description"],
                # language=article["language"].lower(),
                # source_country=article["sourcecountry"].lower(),
                raw_file_source=metadata["source_file"]
            )
            articles.append(article)
    return articles
