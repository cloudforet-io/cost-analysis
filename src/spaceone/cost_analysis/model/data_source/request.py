from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "DataSourceUpdatePermissionsRequest",
]
State = Literal["ENABLED", "DISABLED"]
DataSourceType = Literal["LOCAL", "EXTERNAL"]
SecretType = Literal["MANUAL", "USE_SERVICE_ACCOUNT_SECRET"]
ResourceGroup = Literal["DOMAIN", "WORKSPACE"]


class DataSourceUpdatePermissionsRequest(BaseModel):
    data_source_id: str
    permissions: dict
    domain_id: str
