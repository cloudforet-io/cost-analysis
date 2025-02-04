from datetime import datetime
from typing import Union
from pydantic import BaseModel

__all__ = [
    "BudgetCreateRequest",
    "BudgetUpdateRequest",
    "BudgetSetNotificationRequest",
    "BudgetDeleteRequest",
    "BudgetGetRequest",
    "BudgetSearchQueryRequest",
    "BudgetStatQueryRequest",
]


class BudgetCreateRequest(BaseModel):
    data_source_id: str
    name: Union[str, None] = None
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    provider_filter: Union[dict, None] = None
    time_unit: Union[float, None] = None
    start: Union[datetime, str, None] = None
    end: Union[datetime, str, None] = None
    notifications: Union[list, None] = None
    tags: Union[dict, None] = None
    resource_group: str
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetUpdateRequest(BaseModel):
    budget_id: str
    name: Union[str, None] = None
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetSetNotificationRequest(BaseModel):
    budget_id: str
    notifications: list
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetDeleteRequest(BaseModel):
    budget_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetGetRequest(BaseModel):
    budget_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    budget_id: Union[str, None] = None
    name: Union[str, None] = None
    time_unit: Union[str, None] = None
    data_source_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
