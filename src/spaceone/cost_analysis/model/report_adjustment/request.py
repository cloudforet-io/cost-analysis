from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "CreateReportAdjustmentRequest",
    "UpdateReportAdjustmentRequest",
    "ChangeOrderReportAdjustmentRequest",
    "ReportAdjustmentDeleteRequest",
    "ReportAdjustmentGetRequest",
    "ReportAdjustmentSearchQueryRequest",
]

Unit = Literal["FIXED", "PERCENT"]


class CreateReportAdjustmentRequest(BaseModel):
    name: str
    unit: Unit
    value: float
    description: Union[str, None] = None
    provider: str
    currency: Union[str, None] = "USD"
    order: Union[int, None] = None
    adjustment_filter: Union[dict, None] = None
    report_adjustment_policy_id: str
    domain_id: str


class UpdateReportAdjustmentRequest(BaseModel):
    report_adjustment_id: str
    name: Union[str, None] = None
    unit: Union[Unit, None] = None
    value: Union[float, None] = None
    description: Union[str, None] = None
    provider: Union[str, None] = None
    adjustment_filter: Union[dict, None] = None
    domain_id: str


class ChangeOrderReportAdjustmentRequest(BaseModel):
    report_adjustment_id: str
    order: int
    domain_id: str


class ReportAdjustmentDeleteRequest(BaseModel):
    report_adjustment_id: str
    domain_id: str


class ReportAdjustmentGetRequest(BaseModel):
    report_adjustment_id: str
    domain_id: str


class ReportAdjustmentSearchQueryRequest(BaseModel):
    query: dict
    provider: Union[str, None] = None
    report_adjustment_id: Union[str, None] = None
    report_adjustment_policy_id: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    domain_id: str
