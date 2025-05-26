from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.cost_report_config.request import State, Scope

__all__ = [
    "CostReportConfigResponse",
    "CostReportConfigsResponse",
]


class AdjustmentOptions(BaseModel):
    enabled: bool = False
    period: int = 0


class CostReportConfigResponse(BaseModel):
    cost_report_config_id: Union[str, None] = None
    state: Union[State, None] = None
    scope: Union[Scope, None] = None
    issue_day: Union[int, None] = None
    is_last_day: Union[bool, None] = None
    adjustment_options: Union[AdjustmentOptions, None] = None
    currency: Union[str, None] = None
    recipients: Union[dict, None] = None
    data_source_filter: Union[dict, None] = None
    language: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data["updated_at"])
        return data


class CostReportConfigsResponse(BaseModel):
    results: List[CostReportConfigResponse]
    total_count: int
