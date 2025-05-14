from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PlannedLimit(EmbeddedDocument):
    date = StringField(required=True, max_length=7)
    limit = FloatField(default=0)


class Plan(EmbeddedDocument):
    threshold = FloatField(required=True)
    unit = StringField(max_length=20, required=True, choices=["PERCENT", "ACTUAL_COST"])
    notified = BooleanField(default=False)

    def to_dict(self):
        return dict(self.to_mongo())


class Recipients(EmbeddedDocument):
    users = ListField(StringField(), default=[])
    budget_manager_notification = StringField(
        max_length=20, choices=["ENABLED", "DISABLED"]
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
    state = StringField(
        max_length=20, default="ACTIVE", choices=["SCHEDULED", "ACTIVE", "EXPIRED"]
    )
    limit = FloatField(required=True)
    planned_limits = ListField(EmbeddedDocumentField(PlannedLimit), default=[])
    currency = StringField()
    time_unit = StringField(max_length=20, choices=["TOTAL", "MONTHLY"])
    start = StringField(required=True, max_length=7)
    end = StringField(required=True, max_length=7)
    notification = EmbeddedDocumentField(Notification)
    utilization_rate = FloatField(null=True, default=0)
    tags = DictField(default=None, null=True)
    resource_group = StringField(
        max_length=40, choices=["WORKSPACE", "PROJECT"]
    )  # leave WORKSPACE for previous version
    budget_manager_id = StringField(max_length=60, default=None, null=True)
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
            "state",
            "limit",
            "planned_limits",
            "start",
            "end",
            "notification",
            "utilization_rate",
            "tags",
            "budget_manager_id",
        ],
        "minimal_fields": [
            "budget_id",
            "state",
            "name",
            "limit",
            "utilization_rate",
            "time_unit",
            "currency",
            "budget_manager_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["utilization_rate", "name"],
        "indexes": [
            "domain_id",
            "workspace_id",
            "project_id",
            "name",
            "time_unit",
            "service_account_id",
        ],
    }
