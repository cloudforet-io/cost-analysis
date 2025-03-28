from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PlannedLimit(EmbeddedDocument):
    date = StringField(required=True, max_length=7)
    limit = FloatField(default=0)


class Plan(EmbeddedDocument):
    threshold = FloatField(required=True)
    unit = StringField(max_length=20, required=True, choices=["PERCENT", "ACTUAL_COST"])
    notified_months = ListField(StringField(max_length=10))

    def to_dict(self):
        return dict(self.to_mongo())


class Recipients(EmbeddedDocument):
    users = ListField(StringField(), default=[])
    role_types = ListField(StringField(), default=[])
    service_account_manager = StringField(
        max_length=40, choices=["ENABLED", "DISABLED"], null=True, default=None
    )

    def to_dict(self):
        return dict(self.to_mongo())


class Notification(EmbeddedDocument):
    state = StringField(max_length=20, required=True, choices=["ENABLED", "DISABLED"])
    plans = ListField(EmbeddedDocumentField(Plan), default=[])
    recipients = EmbeddedDocumentField(Recipients)

    def to_dict(self):
        return dict(self.to_mongo())


class Budget(MongoModel):
    budget_id = StringField(max_length=40, generate_id="budget", unique=True)
    name = StringField(max_length=255, default="")
    limit = FloatField(required=True)
    planned_limits = ListField(EmbeddedDocumentField(PlannedLimit), default=[])
    currency = StringField()
    time_unit = StringField(max_length=20, choices=["TOTAL", "MONTHLY"])
    start = StringField(required=True, max_length=7)
    end = StringField(required=True, max_length=7)
    notifications = EmbeddedDocumentField(Notification)
    tags = DictField(default={})
    resource_group = StringField(
        max_length=40, choices=["WORKSPACE", "PROJECT"]
    )  # leave WORKSPACE for previous version
    data_source_id = StringField(max_length=40)
    service_account_id = StringField(max_length=40)
    project_id = StringField(max_length=40, default=None, null=True)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    created_by = StringField(max_length=255, null=True)

    meta = {
        "updatable_fields": [
            "name",
            "limit",
            "planned_limits",
            "notifications",
            "tags",
        ],
        "minimal_fields": [
            "budget_id",
            "name",
            "limit",
            "project_id",
            "service_account_id",
            "data_source_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "resource_group",
            "data_source_id",
            "service_account_id",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
