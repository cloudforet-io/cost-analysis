from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["BudgetResponse", "BudgetsResponse"]


class BudgetResponse(BaseModel):
    budget_id: Union[str, None] = None
    name: Union[str, None] = None
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    currency: Union[str, None] = None
    provider_filter: Union[dict, None] = None
    time_unit: Union[str, None] = None
    start: Union[datetime, None] = None
    end: Union[datetime, None] = None
    notifications: Union[list, None] = None
    tags: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    resource_group: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["start"] = utils.datetime_to_iso8601(data["start"])
        data["end"] = utils.datetime_to_iso8601(data.get("end"))
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class BudgetsResponse(BaseModel):
    results: List[BudgetResponse]
    total_count: int