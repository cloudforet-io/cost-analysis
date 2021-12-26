from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Period(EmbeddedDocument):
    start = StringField(required=True)
    end = StringField(required=True)

    def to_dict(self):
        return dict(self.to_mongo())


class Dashboard(MongoModel):
    dashboard_id = StringField(max_length=40, generate_id='dash', unique=True)
    name = StringField(max_length=255)
    scope = StringField(max_length=20, choices=('PUBLIC', 'PRIVATE'), required=True)
    default_layout_id = StringField(max_length=255, default=None, null=True)
    custom_layouts = ListField(default=[])
    default_filter = DictField(default={})
    period_type = StringField(max_length=20, choices=('AUTO', 'FIXED'), required=True)
    period = EmbeddedDocumentField(Period, default=None, null=True)
    tags = DictField(default={})
    user_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'scope',
            'default_layout_id',
            'custom_layouts',
            'default_filter',
            'period_type',
            'period',
            'tags',
            'user_id'
        ],
        'minimal_fields': [
            'dashboard_id',
            'name',
            'scope',
            'period_type',
            'user_id'
        ],
        'change_query_keys': {
            'user_self': 'user_id'
        },
        'ordering': [
            'scope',
            'name'
        ],
        'indexes': [
            'dashboard_id',
            'name',
            'scope',
            'period_type',
            'user_id',
            'domain_id'
        ]
    }
