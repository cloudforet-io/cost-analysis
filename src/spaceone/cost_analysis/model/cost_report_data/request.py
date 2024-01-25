from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "CostReportDataSearchQueryRequest",
    "CostReportDataAnalyzeQueryRequest",
    "CostReportDataStatQueryRequest",
]

State = Literal["ENABLED", "DISABLED", "PENDING"]
AuthType = Literal["LOCAL", "EXTERNAL"]


class CostReportDataSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_data_id: Union[str, None] = None
    report_year: [str, None] = None
    report_month: [str, None] = None
    product: [str, None] = None
    provider: [str, None] = None
    data_source_id: [str, None] = None
    workspace_id: [str, None] = None
    domain_id: str


class CostReportDataAnalyzeQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_data_id: str
    report_year: [str, None] = None
    report_month: [str, None] = None
    product: [str, None] = None
    provider: [str, None] = None
    data_source_id: [str, None] = None
    workspace_id: [str, None] = None
    domain_id: str


class CostReportDataStatQueryRequest(BaseModel):
    query: dict
    cost_report_data_id: Union[str, None] = None
    workspace_id: [str, None] = None
    domain_id: str
