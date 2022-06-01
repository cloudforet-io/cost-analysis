from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PlannedLimit(EmbeddedDocument):
    date = StringField(required=True)
    limit = FloatField(default=0)


class CostTypes(EmbeddedDocument):
    provider = ListField(StringField(), default=None, null=True)
    region_code = ListField(StringField(), default=None, null=True)
    service_account_id = ListField(StringField(), default=None, null=True)
    product = ListField(StringField(), default=None, null=True)

    def to_dict(self):
        return dict(self.to_mongo())


class Notification(EmbeddedDocument):
    threshold = FloatField(required=True)
    unit = StringField(max_length=20, required=True, choices=('PERCENT', 'ACTUAL_COST'))
    notification_type = StringField(max_length=20, required=True, choices=('CRITICAL', 'WARNING'))


class Budget(MongoModel):
    budget_id = StringField(max_length=40, generate_id='budget', unique=True)
    name = StringField(max_length=255, default='')
    limit = FloatField(required=True)
    planned_limits = ListField(EmbeddedDocumentField(PlannedLimit), default=[])
    total_usage_usd_cost = FloatField(default=0)
    cost_types = EmbeddedDocumentField(CostTypes, default=None, null=True)
    time_unit = StringField(max_length=20, choices=('TOTAL', 'MONTHLY', 'YEARLY'))
    start = StringField(required=True)
    end = StringField(required=True)
    notifications = ListField(EmbeddedDocumentField(Notification), default=[])
    tags = DictField(default={})
    project_id = StringField(max_length=40, default=None, null=True)
    project_group_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'limit',
            'planned_limits',
            'total_usage_usd_cost',
            'end',
            'notifications',
            'tags'
        ],
        'minimal_fields': [
            'budget_id',
            'name',
            'limit',
            'total_usage_usd_cost',
            'project_id',
            'project_group_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id',
            'user_project_groups': 'project_group_id'
        },
        'ordering': ['name'],
        'indexes': [
            # 'budget_id',
            'name',
            'cost_types.provider',
            'cost_types.region_code',
            'cost_types.service_account_id',
            'cost_types.product',
            'time_unit',
            'start',
            'end',
            'project_id',
            'project_group_id',
            'domain_id',
            'created_at'
        ]
    }
