from typing import Union, Literal

from pydantic import BaseModel, Field

TimeUnit = Literal["MONTHLY", "TOTAL"]
ResourceGroup = Literal["WORKSPACE", "PROJECT"]


class BudgetUsageSearchQueryRequest(BaseModel):
    query: dict
    name: Union[str, None] = None
    date: Union[str, None] = None
    budget_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    user_projects: Union[str, None] = None


class BudgetUsageAnalyzeQueryRequest(BaseModel):
    query: dict
    budget_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[str, None] = None


class BudgetUsageStatQueryRequest(BaseModel):
    query: dict
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[str, None] = None
