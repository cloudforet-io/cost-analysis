from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["CostQuerySetResponse", "CostQuerySetsResponse"]


class CostQuerySetResponse(BaseModel):
    cost_query_set_id: Union[str, None] = None
    name: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    user_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data.get("created_at"))
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class CostQuerySetsResponse(BaseModel):
    results: List[CostQuerySetResponse]
    total_count: int