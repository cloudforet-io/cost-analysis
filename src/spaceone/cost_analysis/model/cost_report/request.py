from typing import Union, Literal, List
from pydantic import BaseModel

__all__ = [
    "CostReportSendRequest",
    "CostReportGetUrlRequest",
    "CostReportGetRequest",
    "CostReportSearchQueryRequest",
    "CostReportDataStatQueryRequest",
]

Status = Literal["IN_PROGRESS", "SUCCESS"]


class CostReportSendRequest(BaseModel):
    cost_report_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportGetUrlRequest(BaseModel):
    cost_report_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportGetRequest(BaseModel):
    cost_report_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_id: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    status: Union[Status, None] = None
    issue_date: Union[str, None] = None
    workspace_name: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportDataStatQueryRequest(BaseModel):
    query: dict
    cost_report_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
