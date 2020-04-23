import json
import logging
import uuid

from utils.data.postgres_utils import connect, insert_values
from datetime import datetime
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

    # fetch titles of all stories we haven't done
    # sentiment analysis
    query = """
               SELECT story.uuid, title FROM
               public.apis_story story
               LEFT JOIN
               (SELECT distinct "storyID_id" FROM public.apis_storysentiment
               WHERE is_headline=true) sentiment
               ON story.uuid = sentiment."storyID_id"
               WHERE sentiment."storyID_id" IS NULL
               LIMIT 20000
            """

    response = connect(query)
    values = []
    logging.info("extracting entities from {} articles".format(len(response)))

    count = 1
    for story_uuid, headline in response:
        sentiment = get_sentiment(headline)
        values.append((str(uuid.uuid4()), True,
                       json.dumps(sentiment), story_uuid,
                       str(datetime.now())))
        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    insert_query = """
                      INSERT INTO public.apis_storysentiment
                      (uuid, is_headline, sentiment, "storyID_id", "entryTime")
                      VALUES(%s, %s, %s, %s, %s);
                   """

    insert_values(insert_query, values)
    logging.info("finished")


def sentiment_from_body():
    """
    runs sentiment analysis on each article
    """

    # fetch all stories where body exists and we haven't done
    # sentiment analysis
    query = """
                SELECT story.uuid, body.body FROM
                public.apis_story story
                LEFT JOIN
                (SELECT distinct "storyID_id" FROM public.apis_storysentiment
                WHERE is_headline=false) sentiment
                ON story.uuid = sentiment."storyID_id"
                INNER JOIN (select "storyID_id", (array_agg(body))[1] as body
                from apis_storybody group by "storyID_id") AS body
                ON story.uuid = body."storyID_id"
                WHERE sentiment."storyID_id" IS NULL
                LIMIT 20000
            """

    response = connect(query)
    values = []
    logging.info("extracting entities from {} articles".format(len(response)))

    count = 1
    for story_uuid, body in response:
        sentiment = get_sentiment(body)
        values.append((str(uuid.uuid4()), False,
                       json.dumps(sentiment), story_uuid,
                       str(datetime.now())))
        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    insert_query = """
                      INSERT INTO public.apis_storysentiment
                      (uuid, is_headline, sentiment, "storyID_id", "entryTime")
                      VALUES(%s, %s, %s, %s, %s);
                   """

    insert_values(insert_query, values)
    logging.info("finished")
