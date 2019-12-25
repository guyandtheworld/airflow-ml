import datetime
import mongoengine


class EntityArticleIndex(mongoengine.Document):
    entity_id = mongoengine.IntField()
    entity_name = mongoengine.StringField(required=True)
    entity_tracking_init = mongoengine.DateTimeField(
        default=datetime.datetime.now)
    last_tracked = mongoengine.DateTimeField(required=True)
    total_articles = mongoengine.IntField()
    avg_articles_per_day = mongoengine.IntField()
    history_processed = mongoengine.BooleanField(default=False)
    is_company = mongoengine.BooleanField(default=True)
    actively_tracking = mongoengine.BooleanField(default=True)

    # eventually, risk types
    # risk scores

    meta = {
        'db_alias': 'core',
        'collection': 'testing_company_index'
    }
