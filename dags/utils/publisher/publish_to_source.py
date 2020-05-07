import os
import uuid

import pandas as pd

from datetime import timedelta, datetime
from .publisher import publish
from utils.data.postgres_utils import connect, insert_values


DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BUCKET_NAME = os.getenv("BUCKET_NAME", "news_staging_bucket")
INSERT_QUERY = "INSERT INTO public.apis_lastscrape VALUES(%s, %s, %s, %s)"


def get_last_tracked(row, source):

    # query which we'll use to find out if we've tracked the particular entity
    # if we're tracking, it return the last tracked date for the particular
    # scrape source if it was scraped

    query = """
            select max(lastScraped) as lastScrape from
            (select ls."lastScraped" as lastScraped, ss.name, ls."entityID_id" from
            (public.apis_lastscrape as ls left join
            public.apis_scrapesource as ss
            on ls."scrapeSourceID_id"=ss.uuid)
            where name = '{}' and
            ls."entityID_id" = '{}') fp
            """.format(source, row["entity_id"])

    results = connect(query, verbose=False)

    row["last_tracked"] = results[0][0]
    if not results[0][0]:
        row["history_processed"] = False
    else:
        row["history_processed"] = True

    return row


def publish_to_source(**kwargs):
    """
    publishes companies to scrape to the pubsub
    so news aggregator may process the data
    """
    SOURCE_UUID = kwargs["source_uuid"]
    SOURCE = kwargs["source"]

    # load companies which were added to be tracked
    query = """
                select entity.uuid, entity.name, keywords,
                "scenarioID_id", scenario."trackingDays" from
                public.apis_entity entity
                left join
                public.apis_scenario scenario
                on entity."scenarioID_id" = scenario.uuid
                where "entryVerified"=true;
            """

    results = connect(query, verbose=False)

    df = pd.DataFrame(results, columns=[
                      "entity_id", "name", "keywords", "scenario_id", "trackingDays"])

    df = df.apply(lambda x: get_last_tracked(x, SOURCE), axis=1)

    items_to_insert = []
    for _, row in df.iterrows():
        params = {}
        params["entity_id"] = row["entity_id"]
        params["entity_name"] = row["name"]
        params["common_names"] = row["keywords"]
        params["scenario_id"] = row["scenario_id"]
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
            date_to = datetime.now()
            date_to_write = datetime.strftime(date_to, DATE_FORMAT)
            date_from_write = datetime.strftime(date_from, DATE_FORMAT)
            params["date_from"] = date_from_write
            params["date_to"] = date_to_write

        success = publish(params)

        # if succeeded in publishing update company status & date
        if success:
            items_to_insert.append((str(uuid.uuid4()), str(date_to),
                                    params["entity_id"], SOURCE_UUID,))

    insert_values(INSERT_QUERY, items_to_insert)
