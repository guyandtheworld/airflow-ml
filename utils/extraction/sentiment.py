from data.article import Article
from data.title_analytics import TitleAnalytics
from data.body_analytics import BodyAnalytics
from data.mongo_setup import global_init


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyser = SentimentIntensityAnalyzer()


def get_sentiment(text: str) -> dict:
    sentiment = analyser.polarity_scores(text)
    return sentiment


def get_first_paragraph(text):
    """
    Run sentiment and NER on only the first
    500 words of a news article
    """
    if text is None:
        return ""

    if len(text) > 500:
        return text[:500]
    else:
        return text


def sentiment_analysis_on_headlines():
    """
    runs sentiment analysis on each article
    """

    # setup connection to database
    global_init()
    try:
        articles = Article.objects.filter(
            title_analytics__vader_sentiment_score={})
    except Exception as e:
        print(e)
        raise

    print("extracting sentiment from {} articles".format(len(articles)))
    count = 1
    for article in articles:
        sentiment = get_sentiment(article.title)
        article.update(
            title_analytics__vader_sentiment_score=sentiment)
        if not count % 100:
            print("processed: {}".format(count))
        count += 1
    print("finished")


def sentiment_analysis_on_body():
    """
    runs sentiment analysis on each article
    """

    # setup connection to database
    global_init()
    try:
        articles = Article.objects.filter(
            body_analytics__vader_sentiment_score={})
    except Exception as e:
        print(e)
        raise

    print("extracting sentiment from {} articles".format(len(articles)))
    count = 1
    for article in articles:
        first_para = get_first_paragraph(article.body)
        # running sentiment analysis on first paragraph
        sentiment = get_sentiment(first_para)
        article.update(
            body_analytics__vader_sentiment_score=sentiment)
        if not count % 100:
            print("processed: {}".format(count))
        count += 1
    print("finished")
