from typing import Union
from pydantic import BaseModel, Field
from datetime import datetime

__all__ = ["JobGetTaskRequest"]


class JobGetTaskRequest(BaseModel):
    options: dict
    secret_data: dict
    linked_accounts: Union[list, None] = None
    schema_name: Union[str, None] = Field(None, alias="schema")
    start: Union[str, None] = None
    last_synchronized_at: Union[datetime, None] = None
    domain_id: str
