from typing import Union, Literal

from pydantic import BaseModel, Field

TimeUnit = Literal["MONTHLY", "TOTAL"]
ResourceGroup = Literal["WORKSPACE", "PROJECT"]


class BudgetCreateRequest(BaseModel):
    data_source_id: str
    name: str
    limit: Union[float, None] = None
    planned_limits: Union[list, None] = None
    currency: Union[str, None] = None
    time_unit: TimeUnit
    start: str
    end: str
    notifications: Union[dict, None] = None
    tags: Union[dict, None] = None
    resource_group: ResourceGroup
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetDeleteRequest(BaseModel):
    budget_id: str
    workspace_id: str
    domain_id: str
    user_projects: Union[str, None] = None


class BudgetUsageSearchQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    user_projects: Union[str, None] = None


class BudgetUsageAnalyzeQueryRequest(BaseModel):
    query: dict
    budget_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[str, None] = None


class BudgetUsageStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[str, None] = None
