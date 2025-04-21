from datetime import datetime
from typing import Union, Literal, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.budget.request import ResourceGroup

__all__ = [
    "BudgetResponse",
    "BudgetsResponse",
]


class Plan(BaseModel):
    threshold: float
    unit: str


class Notification(BaseModel):
    state: Union[str, None] = None
    plans: Union[List[Plan], None] = None
    recipients: Union[dict, None] = None


class BudgetResponse(BaseModel):
    budget_id: Union[str, None] = None
    name: Union[str, None] = None
    state: Union[str, None] = None
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    currency: Union[str, None] = None
    time_unit: Union[str, None] = None
    start: Union[str, None] = None
    end: Union[str, None] = None
    notification: Union[Notification, dict] = None
    utilization_rate: Union[float, None] = None
    tags: Union[dict, None] = None
    resource_group: Union[ResourceGroup, None] = None
    budget_manager_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class BudgetsResponse(BaseModel):
    results: List[BudgetResponse]
    total_count: int
