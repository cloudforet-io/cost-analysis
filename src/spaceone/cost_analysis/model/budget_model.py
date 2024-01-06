from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PlannedLimit(EmbeddedDocument):
    date = StringField(required=True, max_length=7)
    limit = FloatField(default=0)


class ProviderFilter(EmbeddedDocument):
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED")
    )
    providers = ListField(StringField(), default=[])

    def to_dict(self):
        return dict(self.to_mongo())


class Notification(EmbeddedDocument):
    threshold = FloatField(required=True)
    unit = StringField(max_length=20, required=True, choices=("PERCENT", "ACTUAL_COST"))
    notification_type = StringField(
        max_length=20, required=True, choices=("CRITICAL", "WARNING")
    )
    notified_months = ListField(StringField(), default=[])

    def to_dict(self):
        return dict(self.to_mongo())


class Budget(MongoModel):
    budget_id = StringField(max_length=40, generate_id="budget", unique=True)
    name = StringField(max_length=255, default="")
    limit = FloatField(required=True)
    planned_limits = ListField(EmbeddedDocumentField(PlannedLimit), default=[])
    currency = StringField()
    provider_filter = EmbeddedDocumentField(ProviderFilter, required=True)
    time_unit = StringField(max_length=20, choices=("TOTAL", "MONTHLY"))
    start = StringField(required=True, max_length=7)
    end = StringField(required=True, max_length=7)
    notifications = ListField(EmbeddedDocumentField(Notification), default=[])
    tags = DictField(default={})
    resource_group = StringField(max_length=40, choices=("WORKSPACE", "PROJECT"))
    data_source_id = StringField(max_length=40)
    project_id = StringField(max_length=40, default=None, null=True)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "limit",
            "planned_limits",
            "provider_filter",
            "notifications",
            "tags",
        ],
        "minimal_fields": [
            "budget_id",
            "name",
            "limit",
            "provider_filter",
            "project_id",
            "data_source_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "resource_group",
            "data_source_id",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
