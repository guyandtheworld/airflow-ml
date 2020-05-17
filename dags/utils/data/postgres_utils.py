import logging
import os
import psycopg2
import json
import time

from google.cloud import pubsub_v1


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "insertion_test")
RESULT = False

logging.basicConfig(level=logging.INFO)

params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}


def connect(query='SELECT version()', verbose=True, args=None):
    """ Connect to the PostgreSQL database server """
    conn = None
    results = []
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        if verbose:
            logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        if verbose:
            logging.info('running : {}'.format(query))

        if args:
            cur.execute(query, args)
        else:
            cur.execute(query)

        # display the PostgreSQL database server version
        results = cur.fetchall()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()

    return results


def insert_values(sql, insert_list):
    """
    insert multiple vendors into the vendors table
    """
    conn = None
    logging.info("inserting: {}".format(len(insert_list)))
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, insert_list)
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()


def update_values(query, update_list):
    """
    insert multiple vendors into the vendors table
    """
    conn = None

    logging.info("updating {} values".format(len(update_list)))
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur, query, update_list, template=None, page_size=100
        )
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()


def delete_values(query, delete_list):
    """
    delete multiple values
    """
    conn = None

    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(query.format(delete_list))
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()


def get_callback(api_future, data, ref):
    """
    Wrap message data in the context of the callback function.
    """

    def callback(api_future):
        global RESULT

        try:
            logging.info(
                "Published message now has message ID {}".format(
                    api_future.result()
                )
            )
            ref["num_messages"] += 1
            RESULT = True
        except Exception:
            RESULT = False

            logging.info(
                "A problem occurred when publishing {}: {}\n".format(
                    data, api_future.exception()
                )
            )
            raise

    return callback


def publish_values(query, values, source):
    """
    insert multiple vendors into the story table
    by sending it over to the insertion service
    """

    payload = {}
    payload["query"] = query
    payload["source"] = source

    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    ref = dict({"num_messages": 0})

    # deliver only maximum of 1000 stories at once
    for i in range(0, len(values), 1000):
        sliced_values = values[i:i+1000]

        payload["data"] = sliced_values

        data = str(json.dumps(payload)).encode('utf-8')

        api_future = client.publish(topic_path, data=data)
        api_future.add_done_callback(get_callback(api_future, data, ref))

        while api_future.running():
            time.sleep(0.5)
            logging.info("Published {} message(s).".format(
                ref["num_messages"]))

    return RESULT
