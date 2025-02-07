from typing import Literal, Union
from pydantic import BaseModel

__all__ = [
    "JobCancelRequest",
    "JobGetRequest",
    "JobSearchQueryRequest",
    "JobStatQueryRequest",
]

Status = Literal["IN_PROGRESS", "SUCCESS", "FAILURE", "TIMEOUT", "CANCELED"]


class JobCancelRequest(BaseModel):
    job_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class JobGetRequest(BaseModel):
    job_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class JobSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    job_id: Union[str, None] = None
    status: Union[Status, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str


class JobStatQueryRequest(BaseModel):
    query: Union[dict, None] = None
    workspace_id: Union[list, str, None] = None
    domain_id: str