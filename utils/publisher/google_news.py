import os
import json

from datetime import timedelta, datetime
from .publisher import publish


SCRAPE_TIMEDELTA = timedelta(hours=1)
SOURCE = "google_news"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BUCKET_NAME = os.getenv("BUCKET_NAME", "news_staging_bucket")


def publish_google_news():
    """
    publishes companies to scrape to the pubsub
    so news aggregator may process the data

    * if new company: scrape based on scenario time-frame
    * else: scrape based on database time-frame
    * each source would have a different last tracked
    """
    # load the active companies to scrape

    codes_dir = '{}/temp_company_db.json'.format(os.path.dirname(__file__))
    with open(codes_dir) as file:
        companies = json.load(file)

    for i in range(len(companies)):
        company = companies[i]

        params = {}
        params["id"] = company["entity_id"]
        params["company_name"] = company["entity_legal_name"]
        params["common_names"] = company["common_names"]
        params["source"] = [SOURCE]
        params["storage_bucket"] = BUCKET_NAME

        if not company["google_news_history_processed"]:
            # date from
            date_from = datetime.now() - \
                timedelta(days=company["scenario_tracking_days"])
            date_from = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from

            # date to
            date_to = datetime.now()
            date_to = datetime.strftime(date_to, DATE_FORMAT)
            params["date_to"] = date_to
        else:
            # date from
            date_from = datetime.strptime(company["google_news_last_tracked"], DATE_FORMAT)

            # date to
            date_to = date_from + SCRAPE_TIMEDELTA
            date_to = datetime.strftime(date_to, DATE_FORMAT)
            date_from = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from
            params["date_to"] = date_to

        success = publish(params)

        # if succeeded in publishing update company status & date
        if success:
            companies[i]["google_news_last_tracked"] = date_to
            if not company["google_news_history_processed"]:
                companies[i]["google_news_history_processed"] = True

    with open(codes_dir, 'w') as file:
        json.dump(companies, file)
