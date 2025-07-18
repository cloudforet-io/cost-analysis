from typing import Union, Literal

from pydantic import BaseModel

__all__ = [
    "CostReportConfigGenerateReportRequest",
    "CostReportConfigCreateRequest",
    "CostReportConfigUpdateRequest",
    "CostReportConfigUpdateRecipientsRequest",
    "CostReportConfigEnableRequest",
    "CostReportConfigDisableRequest",
    "CostReportConfigDeleteRequest",
    "CostReportConfigRunRequest",
    "CostReportConfigGetRequest",
    "CostReportConfigSearchQueryRequest",
    "CostReportConfigStatQueryRequest",
]

State = Literal["ENABLED", "DISABLED"]
Scope = Literal["WORKSPACE", "PROJECT", "SERVICE_ACCOUNT"]


class CostReportConfigGenerateReportRequest(BaseModel):
    cost_report_config_id: str
    report_month: str
    domain_id: str

class CostReportConfigCreateRequest(BaseModel):
    scope: Scope
    issue_day: Union[int, None] = None
    is_last_day: Union[bool, None] = None
    adjustment_options: Union[dict, None] = None
    currency: str
    recipients: dict
    data_source_filter: Union[dict, None] = None
    language: Union[str, None] = None
    domain_id: str


class CostReportConfigUpdateRequest(BaseModel):
    cost_report_config_id: str
    issue_day: Union[int, None] = None
    is_last_day: Union[bool, None] = None
    adjustment_options: Union[dict, None] = None
    currency: Union[str, None] = None
    data_source_filter: Union[dict, None] = None
    language: Union[str, None] = None
    domain_id: str


class CostReportConfigUpdateRecipientsRequest(BaseModel):
    cost_report_config_id: str
    recipients: Union[dict, None] = None
    domain_id: str


class CostReportConfigEnableRequest(BaseModel):
    cost_report_config_id: str
    domain_id: str


class CostReportConfigDisableRequest(BaseModel):
    cost_report_config_id: str
    domain_id: str


class CostReportConfigDeleteRequest(BaseModel):
    cost_report_config_id: str
    domain_id: str


class CostReportConfigRunRequest(BaseModel):
    cost_report_config_id: str
    domain_id: str


class CostReportConfigGetRequest(BaseModel):
    cost_report_config_id: str
    domain_id: str


class CostReportConfigSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    cost_report_config_id: Union[str, None] = None
    state: Union[State, None] = None
    scope: Union[Scope, None] = None
    domain_id: str


class CostReportConfigStatQueryRequest(BaseModel):
    query: dict
    cost_report_config_id: Union[str, None] = None
    domain_id: str
