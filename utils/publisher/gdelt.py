import os
import json

from datetime import timedelta, datetime


SCRAPE_TIMEDELTA = timedelta(hours=9)
SOURCE = "gdelt"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BUCKET_NAME = os.getenv("BUCKET_NAME", "news_staging_bucket")


def publish_gdelt():
    """
    publishes companies to scrape to the pubsub
    so news aggregator may process the data

    * if new company: scrape based on scenario time-frame
    * else: scrape based on database time-frame
    """
    # load the active companies to scrape
    with open('temp_company_db.json') as f:
        companies = json.load(f)

    for i in range(len(companies)):
        company = companies[i]

        params = {}
        params["id"] = company["entity_id"]
        params["company_name"] = company["entity_legal_name"]
        params["common_names"] = company["common_names"]
        params["source"] = SOURCE
        params["storage_bucket"] = BUCKET_NAME

        if not company["history_processed"]:
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
            date_from = datetime.strptime(company["last_tracked"], DATE_FORMAT)

            # date to
            date_to = date_from + SCRAPE_TIMEDELTA
            date_to = datetime.strftime(date_to, DATE_FORMAT)

            date_from = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from

            params["date_to"] = date_to

        # update company status & date
        companies[i]["last_tracked"] = date_to
        print(params)

    with open('temp_company_db.json', "w") as f:
        json.dump(companies, f)


publish_gdelt()
