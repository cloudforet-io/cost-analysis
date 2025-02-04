from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.data_source.request import ResourceGroup
from spaceone.cost_analysis.model.job.request import Status

__all__ = [
    "JobResponse",
    "JobsResponse",
]


class JobResponse(BaseModel):
    job_id: Union[str, None] = None
    status: Union[Status, None] = None
    options: Union[dict, None] = None
    error_code: Union[str, None] = None
    error_message: Union[str, None] = None
    total_tasks: Union[int, None] = None
    remained_tasks: Union[int, None] = None
    changed: Union[list, None] = None
    synced_accounts: Union[list, None] = None
    data_source_id: Union[str, None] = None
    resource_group: ResourceGroup = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None
    finished_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data.get("updated_at"))
        data["finished_at"] = utils.datetime_to_iso8601(
            data.get("finished_at")
        )
        return data


class JobsResponse(BaseModel):
    results: List[JobResponse]
    total_count: int