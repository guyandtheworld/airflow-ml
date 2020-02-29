import json
import logging
import uuid
import en_core_web_sm


from collections import defaultdict
from datetime import datetime
from data.postgres_utils import connect, insert_values


nlp = en_core_web_sm.load()

logging.basicConfig(level=logging.INFO)

# entity_types_to_save = ["PERSON", "NORP", "FACILITY", "ORG", "GPE"]

entity_types_to_save = ["PERSON", "ORG", "GPE"]


def named_entity_recognition(text: str) -> dict:
    """
    recognizes entities in the given text
    """

    document = nlp(text)
    dictionary = defaultdict(list)
    out = [(x.text, x.label_) for x in document.ents]
    for text, label in out:
        dictionary[label].append(text)
    ner = dict(dictionary)

    results = {}
    for key in ner:
        if key in entity_types_to_save:
            results[key] = list(set(ner[key]))

    return {"entities": results}


def entities_from_headlines():
    """
    extracts entities using spaCy from the title
    of the article
    """

    # fetch titles of all stories we haven't done
    # entity recognition
    query = """
                select story.uuid, title from
                public.apis_story story
                left join
                (select * from public.apis_storyentities
                where is_headline=true) entity
                on story.uuid = entity."storyID_id"
                where entities is null
                LIMIT 50000
            """

    response = connect(query)

    count = 1

    values = []
    logging.info("extracting entities from {} articles".format(len(response)))
    for story_uuid, headline in response:
        entities = named_entity_recognition(headline)
        values.append((str(uuid.uuid4()), True,
                       json.dumps(entities), story_uuid,
                       str(datetime.now())))
        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    insert_query = """
                    INSERT INTO public.apis_storyentities
                    (uuid, is_headline, entities, "storyID_id", "entryTime")
                    VALUES(%s, %s, %s, %s, %s);
                   """

    insert_values(insert_query, values)
    logging.info("finished")


def entities_from_body():
    """
    extracts entities using spaCy from the body
    of the article
    """

    # fetch all stories where body exists and we haven't done
    # entity recognition
    query = """
                SELECT story.uuid, body.body FROM
                public.apis_story story
                LEFT JOIN
                (SELECT * FROM public.apis_storyentities
                WHERE is_headline=false) entity
                ON story.uuid = entity."storyID_id"
                LEFT JOIN
                public.apis_storybody AS body
                ON story.uuid = body."storyID_id"
                WHERE entities IS NULL
                AND body IS NOT NULL
                LIMIT 50000
            """

    response = connect(query)

    count = 1

    values = []
    logging.info("extracting entities from {} articles".format(len(response)))
    for story_uuid, body in response:
        entities = named_entity_recognition(body)
        values.append((str(uuid.uuid4()), False,
                       json.dumps(entities), story_uuid,
                       str(datetime.now())))
        if not count % 100:
            logging.info("processed: {}".format(count))
        count += 1

    insert_query = """
                    INSERT INTO public.apis_storyentities
                    (uuid, is_headline, entities, "storyID_id", "entryTime")
                    VALUES(%s, %s, %s, %s, %s);
                   """

    insert_values(insert_query, values)
    logging.info("finished")
