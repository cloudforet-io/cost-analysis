from datetime import datetime
from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class ReportAdjustmentPolicy(MongoModel):
    report_adjustment_policy_id = StringField(
        max_length=40, generate_id="rap", unique=True
    )
    name = StringField(max_length=255, required=True)
    adjustments = ListField(StringField(), default=[])
    scope = StringField(
        max_length=20, choices=("WORKSPACE", "PROJECT"), default="WORKSPACE"
    )
    order = IntField(required=True, default=1)
    tags = DictField(default={})
    policy_filter = DictField(default={"workspace_ids": [], "project_ids": []})
    cost_report_config_id = StringField(max_length=40, required=True)
    domain_id = StringField(max_length=40, required=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "adjustments",
            "scope",
            "order",
            "tags",
            "policy_filter",
            "updated_at",
        ],
        "minimal_fields": [
            "report_adjustment_policy_id",
            "name",
            "scope",
            "order",
            "policy_filter",
            "cost_report_config_id",
            "created_at",
        ],
        "ordering": ["cost_report_config_id", "-order"],
        "indexes": [
            {
                "fields": [
                    "report_adjustment_policy_id",
                    "domain_id",
                ],
                "name": "COMPOUND_INDEX_REPORT_ADJUSTMENT_POLICY",
            }
        ],
    }
