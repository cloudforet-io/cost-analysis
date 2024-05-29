from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["DataSourceAccountResponse", "DataSourceAccountsResponse"]


class DataSourceAccountResponse(BaseModel):
    account_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    name: Union[str, None] = None
    is_sync: Union[bool, None] = None
    is_linked: Union[bool, None] = None
    v_workspace_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class DataSourceAccountsResponse(BaseModel):
    results: List[DataSourceAccountResponse]
    total_count: int
