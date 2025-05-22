from datetime import datetime
from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class ReportAdjustment(MongoModel):
    report_adjustment_id = StringField(max_length=40, generate_id="ra", unique=True)
    name = StringField(max_length=255, required=True)
    unit = StringField(max_length=20, choices=["FIXED", "PERCENT"], required=True)
    value = FloatField(required=True)
    description = StringField(default="")
    provider = StringField(max_length=20, required=True)
    currency = StringField(max_length=10, default="USD")
    order = IntField(default=1)
    adjustment_filter = DictField(default={})
    cost_report_config_id = StringField(max_length=40, required=True)
    report_adjustment_policy_id = StringField(max_length=40, required=True)
    domain_id = StringField(max_length=40, required=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "unit",
            "value",
            "description",
            "provider",
            "currency",
            "order",
            "adjustment_filter",
            "updated_at",
        ],
        "minimal_fields": [
            "report_adjustment_id",
            "name",
            "unit",
            "order",
            "value",
            "adjustment_filter",
        ],
        "ordering": ["report_adjustment_policy_id", "-order"],
        "indexes": [
            {
                "fields": [
                    "report_adjustment_policy_id",
                    "report_adjustment_id",
                    "domain_id",
                ],
                "name": "COMPOUND_INDEX_REPORT_ADJUSTMENT",
            }
        ],
    }
