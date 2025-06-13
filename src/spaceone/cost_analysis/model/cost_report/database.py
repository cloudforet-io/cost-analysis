from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CostReport(MongoModel):
    cost_report_id = StringField(max_length=40, generate_id="cost-report", unique=True)
    cost = DictField(default={})
    status = StringField(
        max_length=20, choices=["IN_PROGRESS", "ADJUSTING", "DONE", "EXPIRED"], required=True
    )
    report_number = StringField(max_length=255)
    currency = StringField(choices=["KRW", "USD", "JPY"], required=True)
    currency_date = StringField(max_length=20)
    issue_date = StringField(max_length=10)
    report_year = StringField(max_length=10)
    report_month = StringField(max_length=10)
    name = StringField(max_length=255)
    bank_name = StringField(max_length=255)
    is_adjusted = BooleanField(default=False)
    cost_report_config_id = StringField(max_length=40)
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=40, default=None, null=True)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "cost",
            "status",
            "is_adjusted",
            "updated_at",
        ],
        "minimal_fields": [
            "cost_report_id",
            "cost",
            "status",
            "report_number",
            "issue_date",
            "is_adjusted",
            "name",
            "workspace_id",
            "domain_id",
        ],
        "ordering": [
            "-created_at",
            "-report_number",
        ],
        "indexes": [
            {
                "fields": [
                    "cost_report_config_id",
                    "status",
                    "domain_id",
                    "workspace_id",
                ],
                "name": "COMPOUND_INDEX_FOR_CONFIRM_REPORT",
            },
            {
                "fields": [
                    "report_month",
                    "status",
                    "domain_id",
                    "workspace_id",
                ],
                "name": "COMPOUND_INDEX_FOR_CONFIRM_REPORT_2",
            },
        ],
    }
