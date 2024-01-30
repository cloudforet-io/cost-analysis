from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CostReport(MongoModel):
    cost_report_id = StringField(max_length=40, generate_id="cost-report", unique=True)
    cost = DictField(default={})
    status = StringField(
        max_length=20, choices=("IN_PROGRESS", "SUCCESS"), default="IN_PROGRESS"
    )
    report_number = StringField(max_length=255)
    currency = StringField(choices=["KRW", "USD", "JPY"], default="KRW")
    currency_date = StringField(max_length=20)
    issue_date = StringField(max_length=10)
    report_year = StringField(max_length=10)
    report_month = StringField(max_length=10)
    workspace_name = StringField(max_length=255)
    bank_name = StringField(max_length=255)
    cost_report_config_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": [
            "status",
        ],
        "minimal_fields": [
            "cost_report_id",
            "status",
            "issue_date",
            "workspace_name",
            "workspace_id",
            "domain_id",
        ],
        "ordering": ["created_at"],
        "indexes": ["cost_report_config_id"],
    }
