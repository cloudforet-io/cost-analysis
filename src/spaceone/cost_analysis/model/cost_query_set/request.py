from datetime import datetime
from typing import Union, Literal
from pydantic import BaseModel, Field

__all__ = [
    "CostQuerySetCreateRequest",
    "CostQuerySetUpdateRequest",
    "CostQuerySetDeleteRequest",
    "CostQuerySetGetRequest",
    "CostQuerySetSearchQueryRequest",
    "CostQuerySetStatQueryRequest",
]


class CostQuerySetCreateRequest(BaseModel):
    data_source_id: str
    name: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostQuerySetUpdateRequest(BaseModel):
    cost_query_set_id: str
    name: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostQuerySetDeleteRequest(BaseModel):
    cost_query_set_id: str
    domain_id: str


class CostQuerySetGetRequest(BaseModel):
    cost_query_set_id: str
    user_id: str
    domain_id: str


class CostQuerySetSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    name: Union[str, None] = None
    data_source_id: Union[str, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class CostQuerySetStatQueryRequest(BaseModel):
    query: dict
    domain_id: str
