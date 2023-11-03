from typing import List, Union
from pydantic import BaseModel

__all__ = ['TasksResponse']


class Task(BaseModel):
    task_options: dict


class Changed(BaseModel):
    start: str
    end: Union[str, None] = None


class TasksResponse(BaseModel):
    tasks: List[Task]
    changed: List[Changed] = []
