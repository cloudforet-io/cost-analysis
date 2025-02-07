from typing import Literal, Union
from pydantic import BaseModel

__all__ = [
    "JobTaskGetRequest",
    "JobTaskSearchQueryRequest",
    "JobTaskStatQueryRequest",
]

Status = Literal["IN_PROGRESS", "SUCCESS", "FAILURE", "TIMEOUT", "CANCELED"]


class JobTaskGetRequest(BaseModel):
    job_task_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class JobTaskSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    job_task_id: Union[str, None] = None
    status: Union[Status, None] = None
    job_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str


class JobTaskStatQueryRequest(BaseModel):
    query: Union[dict, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str