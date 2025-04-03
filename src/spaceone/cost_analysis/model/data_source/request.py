from typing import Union, Literal
from pydantic import BaseModel, Field

__all__ = [
    "DataSourceRegisterRequest",
    "DataSourceUpdateRequest",
    "DataSourceUpdatePermissionsRequest",
    "DataSourceUpdateSecretDataRequest",
    "DataSourceUpdatePluginRequest",
    "DataSourceVerifyPluginRequest",
    "DataSourceEnableRequest",
    "DataSourceDisableRequest",
    "DataSourceDeregisterRequest",
    "DataSourceSyncRequest",
    "DataSourceGetRequest",
    "DataSourceGetRequest",
    "DataSourceSearchQueryRequest",
    "DataSourceStatQueryRequest",
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


class Schedule(BaseModel):
    state: str
    hour: Union[int, None] = None


class DataSourceRegisterRequest(BaseModel):
    name: str
    data_source_type: DataSourceType
    provider: Union[str, None] = None
    secret_type: Union[SecretType, str, None] = None
    secret_filter: Union[dict, None] = None
    template: Union[dict, None] = None
    plugin_info: Union[Plugin, None] = None
    schedule: Schedule
    tags: Union[dict, None] = None
    resource_group: Union[ResourceGroup, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceUpdateRequest(BaseModel):
    data_source_id: str
    name: Union[dict, None] = None
    secret_filter: Union[dict, None] = None
    template: Union[dict, None] = None
    schedule: Schedule
    tags: Union[dict, None] = None
    domain_id: str


class DataSourceUpdatePermissionsRequest(BaseModel):
    data_source_id: str
    permissions: dict
    domain_id: str


class DataSourceUpdateSecretDataRequest(BaseModel):
    data_source_id: str
    secret_data: dict
    secret_schema_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceUpdatePluginRequest(BaseModel):
    data_source_id: str
    version: Union[str, None] = None
    options: Union[dict, None] = None
    upgrade_mode: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceVerifyPluginRequest(BaseModel):
    data_source_id: str
    domain_id: str


class DataSourceEnableRequest(BaseModel):
    data_source_id: str
    domain_id: str


class DataSourceDisableRequest(BaseModel):
    data_source_id: str
    domain_id: str


class DataSourceDeregisterRequest(BaseModel):
    data_source_id: str
    cascade_delete_cost: Union[bool, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceSyncRequest(BaseModel):
    data_source_id: str
    start: Union[str, None] = None
    no_preload_cache: Union[bool, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceGetRequest(BaseModel):
    data_source_id: str
    workspace_id: Union[list, str, None] = None
    domain_id: str


class DataSourceSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    name: Union[str, None] = None
    data_source_type: Union[DataSourceType, None] = None
    provider: Union[str, None] = None
    connected_workspace_id: Union[str, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str

class DataSourceStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
