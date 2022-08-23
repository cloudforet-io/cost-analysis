from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Period(EmbeddedDocument):
    start = StringField(required=True)
    end = StringField(required=True)

    def to_dict(self):
        return dict(self.to_mongo())


class UserDashboard(MongoModel):
    user_dashboard_id = StringField(max_length=40, generate_id='user-dash', unique=True)
    name = StringField(max_length=255)
    default_layout_id = StringField(max_length=255, default=None, null=True)
    custom_layouts = ListField(default=[])
    default_filter = DictField(default={})
    period_type = StringField(max_length=20, choices=('AUTO', 'FIXED'), required=True)
    period = EmbeddedDocumentField(Period, default=None, null=True)
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'default_layout_id',
            'custom_layouts',
            'default_filter',
            'period_type',
            'period',
            'tags',
            'user_id'
        ],
        'minimal_fields': [
            'user_dashboard_id',
            'name',
            'period_type',
            'user_id'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            'name',
            'period_type',
            'user_id',
            'domain_id'
        ]
    }
