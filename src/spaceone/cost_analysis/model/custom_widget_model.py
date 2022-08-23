from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CustomWidget(MongoModel):
    widget_id = StringField(max_length=40, generate_id='widget', unique=True)
    name = StringField(max_length=255)
    options = DictField(default={})
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'options',
            'tags'
        ],
        'minimal_fields': [
            'widget_id',
            'name',
            'user_id'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            'name',
            'user_id',
            'domain_id'
        ]
    }
