import datetime
import mongoengine


class EntityIndex(mongoengine.Document):
    entity_id = mongoengine.IntField(required=True)
    entity_legal_name = mongoengine.StringField(required=True)
    last_tracked = mongoengine.DateTimeField(required=True)
    is_company = mongoengine.BooleanField(default=True)
    actively_tracking = mongoengine.BooleanField(default=True)
    entity_tracking_init = mongoengine.DateTimeField(
        default=datetime.datetime.now)
    entity_common_name = mongoengine.StringField(required=False)
    is_public = mongoengine.BooleanField(required=False)
    ticker = mongoengine.StringField(required=False)
    total_articles = mongoengine.IntField()
    avg_articles_per_day = mongoengine.IntField()
    history_processed = mongoengine.BooleanField(default=False)

    # eventually, risk types
    # risk scores

    meta = {
        'db_alias': 'core',
        'collection': 'test_company_index'
    }
