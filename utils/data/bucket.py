import mongoengine


class Bucket(mongoengine.EmbeddedDocument):
    bucket_id = mongoengine.IntField(required=False)
    model_id = mongoengine.ListField(required=False)

    meta = {
        'db_alias': 'core',
        'collection': 'prod_article_index'
    }
