from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["BudgetUsageResponse", "BudgetUsagesResponse"]


class BudgetUsageResponse(BaseModel):
    budget_id: Union[str, None] = None
    name: Union[str, None] = None
    date: Union[str, None] = None
    cost: Union[float, None] = None
    limit: Union[float, None] = None
    currency: Union[str, None] = None
    provider_filter: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    resource_group: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        return data


class BudgetUsagesResponse(BaseModel):
    results: List[BudgetUsageResponse]
    total_count: int