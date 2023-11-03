from typing import Union
from pydantic import BaseModel
from datetime import datetime

__all__ = ['JobGetTaskRequest']


class JobGetTaskRequest(BaseModel):
    options: dict
    secret_data: dict
    schema: Union[str, None] = None
    start: Union[str, None] = None
    last_synchronized_at: Union[datetime, None] = None
    domain_id: str
