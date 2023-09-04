from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class BudgetUsage(MongoModel):
    budget_id = StringField(max_length=40, required=True)
    name = StringField(max_length=255, default='')
    date = StringField(max_length=7, required=True)
    cost = FloatField(required=True)
    limit = FloatField(required=True)
    currency = StringField(default=None, null=True)
    budget = ReferenceField('Budget', reverse_delete_rule=CASCADE)
    project_id = StringField(max_length=40, default=None, null=True)
    project_group_id = StringField(max_length=40, default=None, null=True)
    data_source_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'cost',
            'limit'
        ],
        'minimal_fields': [
            'budget_id',
            'name',
            'date',
            'usd_cost',
            'limit'
        ],
        'change_query_keys': {
            'user_projects': 'project_id',
            'user_project_groups': 'project_group_id'
        },
        'ordering': ['budget_id', 'date'],
        'indexes': [
            'budget_id',
            'name',
            'date',
            'project_id',
            'project_group_id',
            'data_source_id',
            'domain_id'
        ]
    }
