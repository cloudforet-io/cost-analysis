from datetime import datetime

from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CostReportConfig(MongoModel):
    cost_report_config_id = StringField(max_length=60, generate_id="crc", unique=True)
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED", "DELETED")
    )
    issue_day = IntField(default=10, min_value=1, max_value=31)
    is_last_day = BooleanField(deault=False)
    currency = StringField(max_length=20, default="KRW")
    recipients = DictField(default={})
    data_source_filter = DictField(default={})
    language = StringField(max_length=7, default="ko")
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "state",
            "issue_day",
            "is_last_day",
            "currency",
            "recipients",
            "data_source_filter",
            "language",
            "created_at",
            "updated_at",
            "deleted_at",
        ],
        "minimal_fields": [
            "cost_report_config_id",
            "state",
            "issue_day",
            "created_at",
        ],
        "ordering": ["-created_at"],
        "indexes": ["state", "domain_id"],
    }

    @queryset_manager
    def objects(self, query_set: QuerySet):
        return query_set.filter(state__ne="DELETED")

    def delete(self):
        self.update({"state": "DELETED", "deleted_at": datetime.utcnow()})
