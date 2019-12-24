import mongoengine


def global_init():
    alias = "core"
    name = "testing_alrtai_articles"

    mongoengine.connect(alias=alias, db="", username="", password="",
                        host="", retryWrites="true", w="majority")
