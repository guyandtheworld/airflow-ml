from data.article import Article
from data.title_analytics import TitleAnalytics
from data.mongo_setup import global_init


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyser = SentimentIntensityAnalyzer()


def get_sentiment(text: str) -> dict:
    sentiment = analyser.polarity_scores(text)
    return sentiment


def sentiment_analysis(**params):
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
    count = 0
    for article in articles:
        sentiment = get_sentiment(article.title)
        status = article.update(
            title_analytics__vader_sentiment_score=sentiment)
        if not status:
            print("{} failed".format(article._id))
        if not count % 100:
            print("processed: {}".format(count//100))
        count += 1
    print("finished")
