from typing import Union
from pydantic import BaseModel

__all__ = ['CostGetDataRequest']


class CostGetDataRequest(BaseModel):
    options: dict
    secret_data: dict
    schema: Union[str, None] = None
    task_options: dict
    domain_id: str
