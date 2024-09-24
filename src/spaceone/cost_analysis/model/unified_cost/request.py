from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "UnifiedCostGetRequest",
    "UnifiedCostSearchQueryRequest",
    "UnifiedCostAnalyzeQueryRequest",
    "UnifiedCostStatQueryRequest",
]


class UnifiedCostGetRequest(BaseModel):
    unified_cost_id: str
    users_projects: Union[list, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class UnifiedCostSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    unified_cost_id: Union[str, None] = None
    user_projects: Union[list, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class UnifiedCostAnalyzeQueryRequest(BaseModel):
    query: dict
    is_confirmed: Union[bool, None] = None
    user_projects: Union[list, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class UnifiedCostStatQueryRequest(BaseModel):
    query: dict
    user_projects: Union[list, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
