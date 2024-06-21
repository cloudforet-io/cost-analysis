from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "CostReportDataSearchQueryRequest",
    "CostReportDataAnalyzeQueryRequest",
    "CostReportDataStatQueryRequest",
]


class CostReportDataSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_data_id: Union[str, None] = None
    report_year: Union[str, None] = None
    report_month: Union[str, None] = None
    product: Union[str, None] = None
    provider: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportDataAnalyzeQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_data_id: Union[str, None] = None
    is_confirmed: Union[bool, None] = None
    product: Union[str, None] = None
    provider: Union[str, None] = None
    data_source_id: Union[str, None] = None
    cost_report_id: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    user_projects: Union[list, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class CostReportDataStatQueryRequest(BaseModel):
    query: dict
    cost_report_data_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
