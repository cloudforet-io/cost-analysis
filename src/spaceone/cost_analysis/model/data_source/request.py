from typing import Union, Literal
from pydantic import BaseModel, Field

__all__ = [
    "DataSourceRegisterRequest",
    "DataSourceUpdatePermissionsRequest",
    "DataSourceGetRequest",
    "DataSourceSearchQueryRequest",
]

DataSourceType = Literal["LOCAL", "EXTERNAL"]
State = Literal["ENABLED", "DISABLED"]
SecretType = Literal["MANUAL", "USE_SERVICE_ACCOUNT_SECRET"]
ResourceGroup = Literal["DOMAIN", "WORKSPACE"]


class Plugin(BaseModel):
    plugin_id: str
    version: Union[str, None] = None
    options: Union[dict, None] = None
    metadata: Union[dict, None] = None
    secret_data: Union[dict, None] = None
    schema_id: Union[str, None] = Field(None, alias="schema")
    secret_id: Union[str, None] = None
    upgrade_mode: Union[str, None] = None


class DataSourceRegisterRequest(BaseModel):
    name: str
    data_source_type: DataSourceType
    provider: Union[str, None] = None
    secret_type: Union[SecretType, str, None] = None
    secret_filter: Union[dict, None] = None
    template: Union[dict, None] = None
    plugin_info: Union[Plugin, None] = None
    tags: Union[dict, None] = None
    resource_group: Union[ResourceGroup, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceUpdatePermissionsRequest(BaseModel):
    data_source_id: str
    permissions: dict
    domain_id: str


class DataSourceGetRequest(BaseModel):
    data_source_id: str
    workspace_id: Union[list, str, None] = None
    domain_id: str


class DataSourceSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    name: Union[str, None] = None
    state: Union[State, None] = None
    data_source_type: Union[DataSourceType, None] = None
    provider: Union[str, None] = None
    connected_workspace_id: Union[str, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str
