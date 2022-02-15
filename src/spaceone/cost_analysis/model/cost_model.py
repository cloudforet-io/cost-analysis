from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Cost(MongoModel):
    cost_id = StringField(max_length=40, generate_id='cost')
    usd_cost = FloatField(required=True)
    original_currency = StringField(max_length=40)
    original_cost = FloatField(required=True)
    usage_quantity = FloatField(default=0)
    provider = StringField(max_length=40, default=None, null=True)
    region_code = StringField(max_length=255, default=None, null=True)
    region_key = StringField(max_length=255, default=None, null=True)
    category = StringField(max_length=255, default=None, null=True)
    product = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    usage_type = StringField(max_length=255, default=None, null=True)
    resource_group = StringField(default=None, null=True)
    resource = StringField(default=None, null=True)
    tags = DictField(default={})
    additional_info = DictField(default={})
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    job_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    billed_at = DateTimeField(required=True)
    billed_year = StringField(max_length=20)
    billed_month = StringField(max_length=20)
    billed_date = StringField(max_length=20)

    meta = {
        'updatable_fields': [],
        'minimal_fields': [
            'cost_id',
            'usd_cost',
            'provider',
            'region_code',
            'category',
            'product',
            'account',
            'usage_type',
            'resource_group',
            'resource',
            'data_source_id',
            'billed_at'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'indexes': [
            'cost_id',
            'provider',
            'region_code',
            'region_key',
            'category',
            'product',
            'account',
            'usage_type',
            'resource_group',
            'service_account_id',
            'project_id',
            'data_source_id',
            'job_id',
            'domain_id',
            'billed_at',
            'billed_year',
            'billed_month',
            'billed_date',
            {
                "fields": ['domain_id', 'project_id', 'billed_date'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
        ]
    }


class MonthlyCost(MongoModel):
    usd_cost = FloatField(required=True)
    usage_quantity = FloatField(default=0)
    provider = StringField(max_length=40, default=None, null=True)
    region_code = StringField(max_length=255, default=None, null=True)
    region_key = StringField(max_length=255, default=None, null=True)
    category = StringField(max_length=255, default=None, null=True)
    product = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    usage_type = StringField(max_length=255, default=None, null=True)
    resource_group = StringField(default=None, null=True)
    resource = StringField(default=None, null=True)
    tags = DictField(default={})
    additional_info = DictField(default={})
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    job_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    billed_year = StringField(max_length=20)
    billed_month = StringField(max_length=20)

    meta = {
        'updatable_fields': [],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'indexes': [
            'provider',
            'region_code',
            'region_key',
            'category',
            'product',
            'account',
            'usage_type',
            'resource_group',
            'service_account_id',
            'project_id',
            'data_source_id',
            'job_id',
            'domain_id',
            'billed_year',
            'billed_month',
            {
                "fields": ['domain_id', 'project_id', 'billed_month'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
        ]
    }


class CostQueryHistory(MongoModel):
    query_hash = StringField(max_length=255)
    query_options = DictField(default={})
    domain_id = StringField(max_length=40)
    granularity = StringField(max_length=40)
    start = DateField()
    end = DateField()
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'start',
            'end',
            'updated_at'
        ],
        'indexes': [
            'query_hash',
            'domain_id',
        ]
    }
