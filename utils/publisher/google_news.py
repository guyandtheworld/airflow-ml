import os
import json
import logging

import pandas as pd

from datetime import timedelta, datetime
from .publisher import publish
from data.postgres_utils import connect


SCRAPE_TIMEDELTA = timedelta(hours=1)
SOURCE = "google_news"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BUCKET_NAME = os.getenv("BUCKET_NAME", "news_staging_bucket")
SOURCE_UUID = "1c74e10b-30fd-4052-9c00-eec0fc0ecdcf"


def get_last_tracked(row):

    # query which we'll use to find out if we've tracked the particular entity
    # if we're tracking, it return the last tracked date for the particular
    # scrape source if it was scraped

    query = """
            select max(lastScraped) as lastScrape from
            (select ls."lastScraped" as lastScraped, ss.name, ls."entityID_id" from
            (public.apis_lastscrape as ls left join
            public.apis_scrapesource as ss
            on ls."scrapeSourceID_id"=ss.uuid)
            where name = 'google_news' and
            ls."entityID_id" = '{}') fp
            """.format(row["uuid"])

    results = connect(query)

    row["last_tracked"] = results[0][0]
    if not results[0][0]:
        row["history_processed"] = False
    else:
        row["history_processed"] = True

    return row


def publish_google_news():
    """
    publishes companies to scrape to the pubsub
    so news aggregator may process the data
    """

    # load companies which were added to be tracked - manualEntry
    # and with alias given so that we can scrape it
    query = """
                select entity.uuid, entity.name as legal_name,
                alias.name as alias, scenario."trackingDays" from
                public.apis_entity entity
                left join
                public.apis_scenario scenario
                on entity."scenarioID_id" = scenario.uuid
                full outer join
                public.apis_alias alias
                on entity.uuid = alias."entityID_id"
                where "manualEntry"=true and alias is not null;
            """

    results = connect(query)

    df = pd.DataFrame(results, columns=["uuid", "name", "alias", "trackingDays"])
    df = df.groupby(['uuid', 'name', 'trackingDays'])['alias'].apply(list) \
                    .reset_index()

    df = df.apply(get_last_tracked, axis=1)

    for index, row in df.iterrows():
        params = {}
        params["id"] = row["uuid"]
        params["company_name"] = row["name"]
        params["common_names"] = row["alias"]
        params["source"] = [SOURCE]
        params["storage_bucket"] = BUCKET_NAME
        params["history_processed"] = row["history_processed"]

        if not row["history_processed"]:
            # date from
            date_from = datetime.now() - \
                timedelta(days=int(row["trackingDays"]))
            date_from = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from

            # date to
            date_to = datetime.now()
            date_to_write = datetime.strftime(date_to, DATE_FORMAT)
            params["date_to"] = date_to_write
        else:
            # date from
            date_from = row["last_tracked"]

            # date to
            date_to = date_from + SCRAPE_TIMEDELTA
            date_to_write = datetime.strftime(date_to, DATE_FORMAT)
            date_from_write = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from_write
            params["date_to"] = date_to_write

        # success = publish(params)
        # print(params)

        # if succeeded in publishing update company status & date
        # if success:
        print(params["id"], SOURCE_UUID, date_to)
