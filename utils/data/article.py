import datetime
import mongoengine


from .title_analytics import TitleAnalytics
from .body_analytics import BodyAnalytics


class Article(mongoengine.Document):
    """
    This model will be used to index the articles in relation
    to the Entity that we are using to search
    """
    entity_id = mongoengine.ObjectIdField(
        required=True)  # entity article is related to
    title = mongoengine.StringField(required=True)
    unique_hash = mongoengine.StringField(required=True)
    url = mongoengine.URLField(required=True)
    search_keyword = mongoengine.StringField(required=True)
    published_date = mongoengine.DateTimeField()  # some cases - detected
    description = mongoengine.StringField(required=False)
    body = mongoengine.StringField(required=False)
    internal_source = mongoengine.StringField(required=True)
    domain = mongoengine.URLField(required=True)
    entry_created = mongoengine.DateTimeField(default=datetime.datetime.now)
    language = mongoengine.StringField(required=True)
    source_country = mongoengine.StringField(required=True)
    title_analytics = mongoengine.EmbeddedDocumentField(
        TitleAnalytics, required=False)
    body_analytics = mongoengine.EmbeddedDocumentField(
        BodyAnalytics, required=False)
    raw_file_source = mongoengine.StringField(
        required=True)  # which file the article is in
    has_term_on_title = mongoengine.BooleanField(
        required=False)  # cosine similarity detection
    meta = {
        'db_alias': 'core',
        'collection': 'prod_article_index'
    }
