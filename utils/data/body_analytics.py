import mongoengine


class BodyAnalytics(mongoengine.EmbeddedDocument):
    vader_sentiment_score = mongoengine.DictField()
    entities = mongoengine.DictField()

    meta = {
        'db_alias': 'core',
        'collection': 'prod_article_index'
    }
