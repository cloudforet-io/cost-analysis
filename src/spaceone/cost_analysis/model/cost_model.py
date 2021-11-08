from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Cost(MongoModel):
    cost_id = StringField(max_length=40, generate_id='cost')
    cost_key = StringField(max_length=255, default=None, null=True)
    usd_cost = FloatField(required=True)
    original_currency = StringField(max_length=40)
    original_cost = FloatField(required=True)
    usage_quantity = FloatField(default=0)
    provider = StringField(max_length=40, default=None, null=True)
    region_code = StringField(max_length=255, default=None, null=True)
    product = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    type = StringField(max_length=255, default=None, null=True)
    resource_group = StringField(default=None, null=True)
    resource = StringField(default=None, null=True)
    tags = DictField(default={})
    additional_info = DictField(default={})
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    billed_at = DateTimeField(required=True)

    meta = {
        'updatable_fields': [],
        'minimal_fields': [
            'cost_id',
            'usd_cost',
            'provider',
            'region_code',
            'product',
            'account',
            'type',
            'resource_group',
            'resource',
            'data_source_id',
            'billed_at'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'cost_id',
            'cost_key',
            'usd_cost',
            'original_currency',
            'original_cost',
            'usage_quantity',
            'provider',
            'region_code',
            'product',
            'account',
            'type',
            'resource_group',
            'resource',
            'service_account_id',
            'project_id',
            'data_source_id',
            'domain_id',
            'billed_at'
        ]
    }
