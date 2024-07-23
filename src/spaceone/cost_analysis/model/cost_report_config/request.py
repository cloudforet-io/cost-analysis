from typing import Union, Literal

from pydantic import BaseModel

__all__ = [
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


class CostReportConfigCreateRequest(BaseModel):
    issue_day: Union[int, None] = None
    is_last_day: Union[bool, None] = None
    currency: str = "KRW"
    recipients: dict
    data_source_filter: Union[dict, None] = None
    language: Union[str, None] = None
    domain_id: str


class CostReportConfigUpdateRequest(BaseModel):
    cost_report_config_id: str
    issue_day: Union[int, None] = None
    is_last_day: Union[bool, None] = None
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
    domain_id: str


class CostReportConfigStatQueryRequest(BaseModel):
    query: dict
    cost_report_config_id: Union[str, None] = None
    domain_id: str
