import logging
import os
import psycopg2


logging.basicConfig(level=logging.INFO)

params = {
        'database': os.environ["DB_NAME"],
        'user': os.environ["DB_USER"],
        'password': os.environ["DB_PASSWORD"],
        'host': os.environ["DB_HOST"],
        'port': os.environ["DB_PORT"],
}


def connect(query='SELECT version()'):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        logging.info('running : {}'.format(query))
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


def insert_tracking(track_list):
    """
    insert multiple vendors into the vendors table
    """
    sql = "INSERT INTO public.apis_lastscrape VALUES(%s, %s, %s, %s)"
    conn = None

    logging.info(track_list)
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, track_list)
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_attributes(update_list):
    """
    insert multiple vendors into the vendors table
    """
    conn = None

    query = """
            UPDATE public.apis_story AS t
            SET title_analytics = e.analytics::jsonb
            FROM (VALUES %s) AS e(uuid, analytics)
            WHERE e.uuid = t.uuid::text;
            """

    logging.info(update_list)
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
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    results = connect()
