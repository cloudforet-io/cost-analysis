from typing import Union
from pydantic import BaseModel, Field

__all__ = ['DataSourceInitRequest', 'DataSourceVerifyRequest']


class DataSourceInitRequest(BaseModel):
    options: dict
    domain_id: str


class DataSourceVerifyRequest(BaseModel):
    options: dict
    secret_data: dict
    schema_name: Union[str, None] = Field(None, alias='schema')
    domain_id: str
