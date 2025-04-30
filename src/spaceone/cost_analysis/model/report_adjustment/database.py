from datetime import datetime
from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class ReportAdjustment(MongoModel):
    report_adjustment_id = StringField(max_length=40, generate_id="ra", unique=True)
    name = StringField(max_length=255, required=True)
    method = StringField(max_length=20, choices=("FIXED", "RATE"), required=True)
    value = FloatField(required=True)
    description = StringField(default="")
    provider = StringField(max_length=20, required=True)
    currency = StringField(max_length=10, default="USD")
    order = IntField(default=1)
    filters = DictField(default={})
    cost_report_config_id = StringField(max_length=40, required=True)
    report_adjustment_policy_id = StringField(max_length=40, required=True)
    domain_id = StringField(max_length=40, required=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "name",
            "method",
            "value",
            "description",
            "provider",
            "currency",
            "order",
            "filters",
            "updated_at",
            "deleted_at",
        ],
        "minimal_fields": [
            "report_adjustment_id",
            "name",
            "method",
            "order",
            "value",
        ],
        "ordering": ["-order", "cost_report_config_id"],
        "indexes": [
            "domain_id",
            "report_adjustment_id",
        ],
    }

    @queryset_manager
    def objects(self, queryset: QuerySet):
        return queryset.filter(deleted_at=None)

    def delete(self):
        self.update({"deleted_at": datetime.utcnow()})
