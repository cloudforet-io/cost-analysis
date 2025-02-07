from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.data_source.request import ResourceGroup
from spaceone.cost_analysis.model.job.request import Status

__all__ = [
    "JobTaskResponse",
    "JobTasksResponse",
]


class JobTaskResponse(BaseModel):
    job_task_id: Union[str, None] = None
    status: Union[Status, None] = None
    options: Union[dict, None] = None
    created_count: Union[int, None] = None
    error_code: Union[str, None] = None
    error_message: Union[str, None] = None
    job_id: Union[str, None] = None
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


class JobTasksResponse(BaseModel):
    results: List[JobTaskResponse]
    total_count: int