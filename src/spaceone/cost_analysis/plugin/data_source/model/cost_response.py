from typing import List, Union
from pydantic import BaseModel

__all__ = ['CostsResponse']


class Cost(BaseModel):
    cost: float
    usage_quantity: Union[float, None] = None
    usage_unit: Union[str, None] = None
    provider: Union[str, None] = None
    region_code: Union[str, None] = None
    product: Union[str, None] = None
    usage_type: Union[str, None] = None
    resource: Union[str, None] = None
    tags: dict = {}
    additional_info: dict = {}
    data: dict = {}
    billed_date: str


class CostsResponse(BaseModel):
    results: List[Cost]
