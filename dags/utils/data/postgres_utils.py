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
