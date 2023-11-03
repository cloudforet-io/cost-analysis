from typing import Union
from pydantic import BaseModel, Field

__all__ = ['CostGetDataRequest']


class CostGetDataRequest(BaseModel):
    options: dict
    secret_data: dict
    schema_name: Union[str, None] = Field(None, alias='schema')
    task_options: dict
    domain_id: str
