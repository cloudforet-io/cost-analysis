from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

__all__ = ["CostReportDataResponse", "CostReportsDataResponse"]


class CostReportDataResponse(BaseModel):
    cost_report_data_id: Union[str, None] = None
    cost: Union[dict, None] = None
    cost_report_name: Union[str, None] = None
    issue_date: Union[str, None] = None
    report_year: Union[str, None] = None
    report_month: Union[str, None] = None
    is_confirmed: Union[bool, None] = None
    is_adjusted: Union[bool, None] = None
    provider: Union[str, None] = None
    product: Union[str, None] = None
    region_code: Union[str, None] = None
    region_key: Union[str, None] = None
    usage_type: Union[str, None] = None
    usage_unit: Union[str, None] = None
    service_account_name: Union[str, None] = None
    data_source_name: Union[str, None] = None
    project_name: Union[str, None] = None
    workspace_name: Union[str, None] = None
    service_account_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    cost_report_id: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    report_adjustment_policy_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        return data


class CostReportsDataResponse(BaseModel):
    results: List[CostReportDataResponse]
    total_count: int
