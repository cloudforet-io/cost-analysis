from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class BudgetUsage(MongoModel):
    budget_id = StringField(max_length=40)
    date = StringField()
    usd_cost = FloatField()
    limit = FloatField()
    budget = ReferenceField('Budget', reverse_delete_rule=CASCADE)
    project_id = StringField(max_length=40, default=None, null=True)
    project_group_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'usd_cost',
            'limit'
        ],
        'minimal_fields': [
            'budget_id',
            'date',
            'usd_cost',
            'limit'
        ],
        'change_query_keys': {
            'user_projects': 'project_id',
            'user_project_groups': 'project_group_id'
        },
        'ordering': ['-date'],
        'indexes': [
            'budget_id',
            'date',
            'project_id',
            'project_group_id',
            'domain_id'
        ]
    }
