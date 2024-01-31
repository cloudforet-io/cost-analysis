from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils
from spaceone.cost_analysis.model.cost_report.request import Status

__all__ = ["CostReportResponse", "CostReportsResponse"]


class CostReportResponse(BaseModel):
    cost_report_id: Union[str, None] = None
    cost: Union[dict, None] = None
    status: Union[Status, None] = None
    report_number: Union[str, None] = None
    currency: Union[str, None] = None
    currency_date: Union[str, None] = None
    issue_date: Union[str, None] = None
    report_year: Union[str, None] = None
    report_month: Union[str, None] = None
    workspace_name: Union[str, None] = None
    bank_name: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        return data


class CostReportsResponse(BaseModel):
    results: List[CostReportResponse]
    total_count: int
