from typing import List, Optional, Union, Literal
from datetime import datetime
from pydantic import BaseModel
from spaceone.core import utils

__all__ = [
    "ReportAdjustmentPolicyResponse",
    "ReportAdjustmentPoliciesResponse",
]

State = Literal["ENABLED", "DELETED", "DISABLED"]


class ReportAdjustmentPolicyResponse(BaseModel):
    report_adjustment_policy_id: Union[str, None] = None
    name: Union[str, None] = None
    scope: Union[str, None] = None
    order: Union[int, None] = None
    state: Union[State, None] = None
    adjustments: Union[list, None] = None
    tags: Union[dict, None] = None

    cost_report_config_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    workspace_ids: Union[list, None] = None
    project_ids: Union[list, None] = None

    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data.get("created_at"))
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        data["deleted_at"] = utils.datetime_to_iso8601(data.get("deleted_at"))
        return data


class ReportAdjustmentPoliciesResponse(BaseModel):
    results: List[ReportAdjustmentPolicyResponse]
    total_count: int
