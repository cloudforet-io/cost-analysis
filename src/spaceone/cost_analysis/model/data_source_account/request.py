from typing import Union
from pydantic import BaseModel

__all__ = [
    "DataSourceAccountUpdateRequest",
    "DataSourceAccountResetRequest",
    "DataSourceAccountGetRequest",
    "DataSourceAccountSearchQueryRequest",
    "DataSourceAnalyzeQueryRequest",
    "DataSourceAccountStatQueryRequest",
]


class DataSourceAccountUpdateRequest(BaseModel):
    data_source_id: str
    account_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceAccountResetRequest(BaseModel):
    data_source_id: str
    account_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceAccountGetRequest(BaseModel):
    data_source_id: str
    account_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceAnalyzeQueryRequest(BaseModel):
    query: dict
    data_source_id: Union[str, None] = None
    account_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceAccountSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    account_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceAccountStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
