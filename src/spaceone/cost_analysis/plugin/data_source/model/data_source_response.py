from typing import List
from enum import Enum
from pydantic import BaseModel

__all__ = ['PluginResponse']


class MatchServiceAccount(BaseModel):
    source: str
    target: str


class MatchProject(BaseModel):
    source: str
    target: str


class Actions(BaseModel):
    match_service_account: MatchServiceAccount
    match_project: MatchProject
    change_project: str
    add_additional_info: dict


class Condition(BaseModel):
    key: str
    value: str
    operator: str


class Options(BaseModel):
    stop_processing: bool


class State(str, Enum):
    all = 'ALL'
    any = 'ANY'
    always = 'ALWAYS'


class DataSourceRule(BaseModel):
    name: str
    conditions: List[Condition] = []
    conditions_policy: State
    actions: Actions
    options: Options = {}
    tags: dict


class SupportedSecretType(str, Enum):
    manual = 'MANUAL'
    use_service_account_secret = 'USE_SERVICE_ACCOUNT_SECRET'


class PluginMetadata(BaseModel):
    data_source_rules: List[DataSourceRule]
    supported_secret_types: List[SupportedSecretType] = [SupportedSecretType.manual]
    currency: str = 'USD'


class PluginResponse(BaseModel):
    metadata: PluginMetadata

    class Config:
        use_enum_values = True
