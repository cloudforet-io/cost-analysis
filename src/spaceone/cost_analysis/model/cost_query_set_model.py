from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CostQuerySet(MongoModel):
    cost_query_set_id = StringField(max_length=40, generate_id='query', unique=True)
    name = StringField(max_length=255)
    scope = StringField(max_length=20, choices=('PUBLIC', 'PRIVATE'), default='PRIVATE')
    options = DictField(default={})
    tags = DictField(default={})
    user_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'scope',
            'options',
            'tags',
            'user_id'
        ],
        'minimal_fields': [
            'cost_query_set_id',
            'name',
            'scope',
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
            'cost_query_set_id',
            'name',
            'scope',
            'user_id',
            'domain_id'
        ]
    }
