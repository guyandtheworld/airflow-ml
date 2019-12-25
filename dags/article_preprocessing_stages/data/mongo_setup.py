import mongoengine
import os


def global_init():
    alias = "core"
    name = "testing_articles"

    try:
        data = {
            "db": os.environ["db"],
            "username": os.environ["username"],
            "password": os.environ["password"],
            "host": os.environ["host"],
            "retryWrites": os.environ["retryWrites"],
            "w": os.environ["w"],
        }
    except KeyError as e:
        resp = {"status": "os error, no env variables",
                "error": e}
        print(resp)

    mongoengine.connect(alias=alias,
                        name=name,
                        **data)
