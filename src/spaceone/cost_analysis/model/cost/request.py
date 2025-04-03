from typing import Union
from pydantic import BaseModel

__all__ = [
    "CostCreateRequest",
    "CostDeleteRequest",
    "CostGetRequest",
    "CostSearchQueryRequest",
    "CostAnalyzeQueryRequest",
    "CostStatQueryRequest",
]


class CostCreateRequest(BaseModel):
    cost: float
    usage_quantity: Union[float, None] = None
    usage_unit: Union[str, None] = None
    provider: Union[str, str, None] = None
    region_code: Union[str, None] = None
    product: Union[str, None] = None
    usage_type: Union[str, None] = None
    resource: Union[str, None] = None
    tags: Union[dict, None] = None
    additional_info: Union[dict, None] = None
    data_source_id: str
    billed_date: str
    service_account_id: Union[str, None] = None
    project_id: str
    workspace_id: str
    domain_id: str


class CostDeleteRequest(BaseModel):
    data_source_id: str
    cost_id: str
    workspace_id: str
    domain_id: str


class CostGetRequest(BaseModel):
    data_source_id: str
    cost_id: str
    user_projects: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class CostSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    cost_id: Union[str, None] = None
    provider: Union[str, None] = None
    region_code: Union[str, None] = None
    region_key: Union[str, None] = None
    product: Union[str, None] = None
    usage_type: Union[str, None] = None
    resource: Union[str, None] = None
    billed_year: Union[str, None] = None
    billed_month: Union[str, None] = None
    billed_date: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    project_group_id: Union[str, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str


class CostAnalyzeQueryRequest(BaseModel):
    data_source_id: str
    query: dict
    workspace_id: Union[list, str, None] = None
    domain_id: str


class CostStatQueryRequest(BaseModel):
    data_source_id: Union[str, None] = None
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
