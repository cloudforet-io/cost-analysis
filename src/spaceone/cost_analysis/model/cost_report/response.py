from typing import Union, Literal, List
from pydantic import BaseModel
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


class CostReportsResponse(BaseModel):
    results: List[CostReportResponse]
    total_count: int
