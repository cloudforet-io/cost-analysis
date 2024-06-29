from datetime import datetime
from typing import Union, Literal, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.data_source.request import (
    State,
    DataSourceType,
    SecretType,
    ResourceGroup,
)

__all__ = [
    "DataSourceResponse",
    "DataSourcesResponse",
]


class DataSourceResponse(BaseModel):
    data_source_id: Union[str, None] = None
    name: Union[str, None] = None
    state: Union[State, None] = None
    data_source_type: Union[DataSourceType, None] = None
    permissions: Union[dict, None] = None
    provider: Union[str, None] = None
    secret_type: Union[SecretType, None] = None
    secret_filter: Union[dict, None] = None
    plugin_info: Union[dict, None] = None
    template: Union[dict, None] = None
    tags: Union[dict, None] = None
    cost_tag_keys: Union[list, None] = None
    cost_additional_info_keys: Union[list, None] = None
    cost_data_keys: Union[list, None] = None
    data_source_account_count: Union[int, None] = None
    connected_workspace_count: Union[int, None] = None
    resource_group: Union[ResourceGroup, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None
    last_synchronized_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        data["last_synchronized_at"] = utils.datetime_to_iso8601(
            data.get("last_synchronized_at")
        )
        return data


class DataSourcesResponse(BaseModel):
    results: List[DataSourceResponse]
    total_count: int
