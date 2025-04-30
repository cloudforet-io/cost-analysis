from datetime import datetime
from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class ReportAdjustmentPolicy(MongoModel):
    report_adjustment_policy_id = StringField(
        max_length=40, generate_id="rap", unique=True
    )
    name = StringField(max_length=255, required=True)
    state = StringField(
        max_length=20, choices=("ENABLED", "DISABLED", "DELETED"), default="ENABLED"
    )
    adjustments = ListField(StringField(), default=[])
    scope = StringField(
        max_length=20, choices=("WORKSPACE", "PROJECT"), default="WORKSPACE"
    )
    order = IntField(required=True, default=1)
    tags = DictField(default={})
    cost_report_config_id = StringField(max_length=40, required=True)
    domain_id = StringField(max_length=40, required=True)
    workspace_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "name",
            "adjustments",
            "scope",
            "order",
            "state",
            "tags",
            "updated_at",
            "deleted_at",
            "workspace_id",
            "project_id",
        ],
        "minimal_fields": [
            "report_adjustment_policy_id",
            "name",
            "scope",
            "order",
            "cost_report_config_id",
            "state",
            "created_at",
        ],
        "ordering": ["cost_report_config_id", "-order"],
        "indexes": [
            "domain_id",
            "report_adjustment_policy_id",
            "scope",
            "state",
            "workspace_id",
            "project_id",
        ],
    }

    @queryset_manager
    def objects(self, queryset: QuerySet):
        return queryset.filter(state__ne="DELETED")

    def delete(self):
        self.update({"state": "DELETED", "deleted_at": datetime.utcnow()})
