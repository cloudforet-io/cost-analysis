from typing import List, Optional, Union
from datetime import datetime
from pydantic import BaseModel
from spaceone.core import utils

from spaceone.cost_analysis.model.report_adjustment.request import Unit

__all__ = [
    "ReportAdjustmentResponse",
    "ReportAdjustmentsResponse",
]


class ReportAdjustmentResponse(BaseModel):
    report_adjustment_id: Union[str, None] = None
    name: Union[str, None] = None
    unit: Union[Unit, None] = None
    value: Union[float, None] = None
    description: Union[str, None] = None
    provider: Union[str, None] = None
    currency: Union[str, None] = None
    order: Union[int, None] = None
    adjustment_filter: Union[dict, None] = None
    cost_report_config_id: Union[str, None] = None
    report_adjustment_policy_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data.get("created_at"))
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class ReportAdjustmentsResponse(BaseModel):
    results: List[ReportAdjustmentResponse]
    total_count: int
