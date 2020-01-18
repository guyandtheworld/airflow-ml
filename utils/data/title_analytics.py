import mongoengine


class TitleAnalytics(mongoengine.EmbeddedDocument):
    entities = mongoengine.DictField(required=False)
    vader_sentiment_score = mongoengine.DictField(required=False)

    meta = {
        'db_alias': 'core',
        'collection': 'prod_article_index'
    }
