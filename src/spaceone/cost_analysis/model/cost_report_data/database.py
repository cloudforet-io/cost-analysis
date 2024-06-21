from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CostReportData(MongoModel):
    cost_report_data_id = StringField(max_length=40, generate_id="crd", unique=True)
    cost = DictField(default={})
    cost_report_name = StringField(max_length=255)
    issue_date = StringField(max_length=20)
    report_year = StringField(max_length=20)
    report_month = StringField(max_length=40)
    is_confirmed = BooleanField(default=False)
    provider = StringField(max_length=40)
    product = StringField(max_length=255)
    service_account_name = StringField(max_length=255)
    data_source_name = StringField(max_length=255)
    project_name = StringField(max_length=255)
    workspace_name = StringField(max_length=255)
    service_account_id = StringField(max_length=40)
    data_source_id = StringField(max_length=40)
    cost_report_id = StringField(max_length=40)
    cost_report_config_id = StringField(max_length=40)
    project_id = StringField(max_length=40, default=None, null=True)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": [],
        "minimal_fields": [
            "cost_report_config_id",
            "cost_report_data_id",
            "data_source_id",
            "workspace_id",
        ],
        "ordering": ["is_confirmed", "-report_year"],
        "change_query_keys": {"user_projects": "project_id"},
        "indexes": [
            {
                "fields": [
                    "cost_report_config_id",
                    "cost_report_id",
                    "domain_id",
                ],
                "name": "COMPOUND_INDEX_FOR_DEFAULT_COST_REPORT_DATA",
            },
            {
                "fields": ["is_confirmed", "domain_id", "workspace_id", "project_id"],
                "name": "COMPOUND_INDEX_FOR_WORKSPACE_LANDING",
            },
        ],
    }
