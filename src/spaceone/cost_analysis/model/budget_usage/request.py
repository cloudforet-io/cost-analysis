from typing import Union
from pydantic import BaseModel

__all__ = [
    "BudgetUsageSearchQueryRequest",
    "BudgetUsageAnalyzeQueryRequest",
    "BudgetUsageStatQueryRequest",
]


class BudgetUsageSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    name: Union[str, None] = None
    date: Union[str, None] = None
    budget_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetUsageAnalyzeQueryRequest(BaseModel):
    query: dict
    budget_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class BudgetUsageStatQueryRequest(BaseModel):
    query: dict
    budget_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
