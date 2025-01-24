from typing import List, Union
from pydantic import BaseModel

__all__ = ["TasksResponse"]


class Changed(BaseModel):
    start: str
    end: Union[str, None] = None
    filter: Union[dict, None] = None


class Task(BaseModel):
    task_options: dict
    task_changed: Union[Changed, None] = None


class SyncedAccount(BaseModel):
    account_id: str
    name: Union[str, None] = None


class TasksResponse(BaseModel):
    tasks: List[Task]
    changed: List[Changed] = []
    synced_accounts: Union[List[SyncedAccount], None] = None
