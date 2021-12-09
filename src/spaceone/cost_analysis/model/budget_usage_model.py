from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CostTypes(EmbeddedDocument):
    provider = ListField(StringField(), default=None, null=True)
    region_code = ListField(StringField(), default=None, null=True)
    service_account_id = ListField(StringField(), default=None, null=True)
    product = ListField(StringField(), default=None, null=True)

    def to_dict(self):
        return dict(self.to_mongo())


class BudgetUsage(MongoModel):
    budget_id = StringField(max_length=40)
    name = StringField(max_length=255, default='')
    date = StringField()
    usd_cost = FloatField()
    limit = FloatField()
    cost_types = EmbeddedDocumentField(CostTypes, default=None, null=True)
    budget = ReferenceField('Budget', reverse_delete_rule=CASCADE)
    project_id = StringField(max_length=40, default=None, null=True)
    project_group_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'usd_cost',
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
            'cost_types.provider',
            'cost_types.region_code',
            'cost_types.service_account_id',
            'cost_types.product',
            'project_id',
            'project_group_id',
            'domain_id'
        ]
    }
