import json
import logging
import uuid

from data.postgres_utils import connect, insert_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


analyser = SentimentIntensityAnalyzer()

logging.basicConfig(level=logging.INFO)


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


def sentiment_on_headlines():
    """
    runs sentiment analysis on each article
    """

    query = """
               SELECT story.uuid, title FROM
               public.apis_story story
               LEFT JOIN
               (SELECT * FROM public.apis_storysentiment
               WHERE is_headline=true) entity
               ON story.uuid = entity."storyID_id"
               WHERE sentiment IS NULL
               LIMIT 20000
            """

    response = connect(query)
    values = []
    logging.info("extracting entities from {} articles".format(len(response)))

    count = 1
    for story_uuid, headline in response:
        sentiment = get_sentiment(headline)
        values.append((str(uuid.uuid4()), True,
                       json.dumps(sentiment), story_uuid))
        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    insert_query = """
                      INSERT INTO public.apis_storysentiment
                      (uuid, is_headline, sentiment, "storyID_id")
                      VALUES(%s, %s, %s, %s);
                   """

    insert_values(insert_query, values)
    logging.info("finished")


def sentiment_from_body():
    """
    runs sentiment analysis on each article
    """

    # setup connection to database
    # global_init()
    # try:
    #     articles = Article.objects.filter(
    #         body_analytics__vader_sentiment_score={})
    # except Exception as e:
    #     print(e)
    #     raise

    # print("extracting sentiment from {} articles".format(len(articles)))
    # count = 1
    # for article in articles:
    #     first_para = get_first_paragraph(article.body)
    #     # running sentiment analysis on first paragraph
    #     sentiment = get_sentiment(first_para)
    #     article.update(
    #         body_analytics__vader_sentiment_score=sentiment)
    #     if not count % 100:
    #         print("processed: {}".format(count))
    #     count += 1
    print("finished")
