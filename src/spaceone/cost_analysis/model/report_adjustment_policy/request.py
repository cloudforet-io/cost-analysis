from typing import Literal, Union
from pydantic import BaseModel

__all__ = [
    "CreateReportAdjustmentPolicyRequest",
    "UpdateReportAdjustmentPolicyRequest",
    "ChangeOrderReportAdjustmentPolicyRequest",
    "ReportAdjustmentPolicyDeleteRequest",
    "ReportAdjustmentPolicySyncCurrencyRequest",
    "ReportAdjustmentPolicyGetRequest",
    "ReportAdjustmentPolicySearchQueryRequest",
]


class CreateReportAdjustmentPolicyRequest(BaseModel):
    cost_report_config_id: str
    order: Union[int, None] = None
    description: Union[None, str] = None
    tags: Union[dict, None] = None
    policy_filter: Union[dict, None] = None
    domain_id: str


class UpdateReportAdjustmentPolicyRequest(BaseModel):
    report_adjustment_policy_id: str
    description: Union[str, None] = None
    tags: Union[dict, None] = None
    policy_filter: Union[dict, None] = None
    domain_id: str


class ChangeOrderReportAdjustmentPolicyRequest(BaseModel):
    report_adjustment_policy_id: str
    order: int
    domain_id: str


class ReportAdjustmentPolicyDeleteRequest(BaseModel):
    report_adjustment_policy_id: str
    domain_id: str


class ReportAdjustmentPolicySyncCurrencyRequest(BaseModel):
    report_adjustment_policy_id: str
    domain_id: str


class ReportAdjustmentPolicyGetRequest(BaseModel):
    report_adjustment_policy_id: str
    domain_id: str


class ReportAdjustmentPolicySearchQueryRequest(BaseModel):
    query: dict
    report_adjustment_policy_id: Union[str, None] = None
    domain_id: str
