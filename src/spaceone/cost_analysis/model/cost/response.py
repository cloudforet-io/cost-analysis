from typing import Union, List
from pydantic import BaseModel

__all__ = ["CostResponse", "CostsResponse"]


class CostResponse(BaseModel):
    cost_id: Union[str, None] = None
    cost: Union[float, None] = None
    usage_quantity: Union[float, None] = None
    usage_unit: Union[str, None] = None
    provider: Union[str, None] = None
    region_code: Union[str, None] = None
    region_key: Union[str, None] = None
    product: Union[str, None] = None
    usage_type: Union[str, None] = None
    resource: Union[str, None] = None
    tags: Union[dict, None] = None
    additional_info: Union[dict, None] = None
    data: Union[dict, None] = None
    account_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    billed_year: Union[str, None] = None
    billed_month: Union[str, None] = None
    billed_date: Union[str, None] = None


class CostsResponse(BaseModel):
    results: List[CostResponse]
    total_count: int
