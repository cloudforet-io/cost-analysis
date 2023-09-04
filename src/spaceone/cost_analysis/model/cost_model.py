from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Cost(MongoModel):
    cost_id = StringField(max_length=40, generate_id='cost')
    cost = FloatField(required=True)
    usage_quantity = FloatField(default=0)
    usage_unit = StringField(max_length=255, default=None, null=True)
    provider = StringField(max_length=40, default=None, null=True)
    region_code = StringField(max_length=255, default=None, null=True)
    region_key = StringField(max_length=255, default=None, null=True)
    product = StringField(max_length=255, default=None, null=True)
    usage_type = StringField(max_length=255, default=None, null=True)
    resource = StringField(default=None, null=True)
    tags = DictField(default={})
    additional_info = DictField(default={})
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    job_id = StringField(max_length=40, default=None, null=True)
    job_task_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    billed_year = StringField(max_length=4, required=True)
    billed_month = StringField(max_length=7, required=True)
    billed_date = StringField(max_length=10, required=True)

    meta = {
        'updatable_fields': [],
        'minimal_fields': [
            'cost_id',
            'cost',
            'provider',
            'region_code',
            'product',
            'usage_type',
            'resource',
            'data_source_id',
            'billed_date'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'indexes': [
            {
                "fields": ['domain_id', 'data_source_id', 'job_id', 'job_task_id', '-billed_month'],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB"
            },
            {
                "fields": ['domain_id', 'data_source_id', '-billed_date', 'project_id', 'cost'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
            {
                "fields": ['domain_id', 'cost_id', 'project_id'],
                "name": "COMPOUND_INDEX_FOR_DELETE"
            }
        ]
    }


class MonthlyCost(MongoModel):
    cost = FloatField(required=True)
    usage_quantity = FloatField(default=0)
    usage_unit = StringField(max_length=255, default=None, null=True)
    provider = StringField(max_length=40, default=None, null=True)
    region_code = StringField(max_length=255, default=None, null=True)
    region_key = StringField(max_length=255, default=None, null=True)
    product = StringField(max_length=255, default=None, null=True)
    usage_type = StringField(max_length=255, default=None, null=True)
    resource = StringField(default=None, null=True)
    tags = DictField(default={})
    additional_info = DictField(default={})
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    job_id = StringField(max_length=40, default=None, null=True)
    job_task_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    billed_year = StringField(max_length=4, required=True)
    billed_month = StringField(max_length=7, required=True)

    meta = {
        'updatable_fields': [],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'indexes': [
            {
                "fields": ['domain_id', 'data_source_id', 'job_id', 'job_task_id', '-billed_month'],
                "name": "COMPOUND_INDEX_FOR_SYNC_JOB"
            },
            {
                "fields": ['domain_id', 'data_source_id', '-billed_month', 'project_id', 'cost'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"
            },
            {
                "fields": ['domain_id', 'data_source_id', '-billed_year', 'project_id', 'cost'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"
            },
        ]
    }


class CostQueryHistory(MongoModel):
    query_hash = StringField(max_length=255)
    query_options = DictField(default={})
    data_source_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'updated_at'
        ],
        'indexes': [
            {
                "fields": ['domain_id', 'domain_id', 'query_hash'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
        ]
    }
