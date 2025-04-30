from typing import Literal, Union
from pydantic import BaseModel

__all__ = [
    "CreateReportAdjustmentPolicyRequest",
    "UpdateReportAdjustmentPolicyRequest",
    "ChangeOrderReportAdjustmentPolicyRequest",
    "ReportAdjustmentPolicyDeleteRequest",
    "ReportAdjustmentPolicyGetRequest",
    "ReportAdjustmentPolicySearchQueryRequest",
]

SCOPE = Literal["WORKSPACE", "PROJECT"]


class CreateReportAdjustmentPolicyRequest(BaseModel):
    name: str
    scope: SCOPE
    cost_report_config_id: str
    order: Union[int, None] = None
    tags: Union[dict, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class UpdateReportAdjustmentPolicyRequest(BaseModel):
    report_adjustment_policy_id: str
    name: Union[str, None] = None
    tags: Union[dict, None] = None
    domain_id: str


class ChangeOrderReportAdjustmentPolicyRequest(BaseModel):
    report_adjustment_policy_id: str
    order: int
    domain_id: str


class ReportAdjustmentPolicyDeleteRequest(BaseModel):
    report_adjustment_policy_id: str
    domain_id: str


class ReportAdjustmentPolicyGetRequest(BaseModel):
    report_adjustment_policy_id: str
    domain_id: str


class ReportAdjustmentPolicySearchQueryRequest(BaseModel):
    query: dict
    name: Union[str, None] = None
    state: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
