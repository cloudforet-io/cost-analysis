from typing import Union, Literal

from pydantic import BaseModel, Field

TimeUnit = Literal["MONTHLY", "TOTAL"]
ResourceGroup = Literal["WORKSPACE", "PROJECT"]


class BudgetCreateRequest(BaseModel):
    name: str
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    currency: str
    time_unit: TimeUnit
    start: str
    end: str
    notification: Union[dict, None] = None
    tags: Union[dict, None] = None
    resource_group: ResourceGroup
    budget_manager_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetUpdateRequest(BaseModel):
    budget_id: str
    name: Union[str, None] = None
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    start: Union[str, None] = None
    end: Union[str, None] = None
    tags: Union[dict, None] = None
    budget_manager_id: Union[str, None] = None
    workspace_id: str
    domain_id: str


class BudgetSetNotificationRequest(BaseModel):
    budget_id: str
    notification: dict
    project_id: Union[str, None] = None
    workspace_id: str
    domain_id: str


class BudgetGetRequest(BaseModel):
    budget_id: str
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class BudgetDeleteRequest(BaseModel):
    budget_id: str
    workspace_id: str
    domain_id: str


class BudgetSearchQueryRequest(BaseModel):
    query: dict
    name: Union[str, None] = None
    time_unit: Union[TimeUnit, None] = None
    budget_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class BudgetStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
