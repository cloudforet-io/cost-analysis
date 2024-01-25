from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CostReportData(MongoModel):
    cost_report_data_id = StringField(max_length=40, generate_id="cdr", unique=True)
    cost = DictField(default={})
    cost_report_name = StringField(max_length=255)
    report_year = StringField(max_length=20)
    report_month = StringField(max_length=40)
    is_confirmed = BooleanField(default=False)
    provider = StringField(max_length=40)
    product = StringField(max_length=40)
    service_account_name = StringField(max_length=255)
    data_source_name = StringField(max_length=255)
    project_name = StringField(max_length=40)
    workspace_name = StringField(max_length=40)
    service_account_id = StringField(max_length=40)
    data_source_id = StringField(max_length=40)
    cost_report_id = StringField(max_length=40)
    cost_report_config_id = StringField(max_length=40)
    workspace_id = StringField(
        max_length=40, default=None, null=True
    )  # todo workspace_id required
    domain_id = StringField(max_length=40)

    meta = {
        "updatable_fields": [],
        "minimal_fields": [
            "cost_report_config_id",
            "cost_report_data_id",
            "data_source_id",
            "workspace_id",
        ],
        "ordering": ["is_confirmed"],
        "indexes": [
            "cost_report_config_id",
            "cost_report_id",
            "-report_year",
        ],
    }
