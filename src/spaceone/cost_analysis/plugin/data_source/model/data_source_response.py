from typing import List, Union, Dict
from enum import Enum
from pydantic import BaseModel

__all__ = ["PluginResponse"]


class MatchServiceAccount(BaseModel):
    source: str
    target: str


class MatchProject(BaseModel):
    source: str
    target: str


class MatchWorkspace(BaseModel):
    source: str
    target: str


class Actions(BaseModel):
    match_workspace: Union[MatchWorkspace] = None
    match_service_account: Union[MatchServiceAccount, None] = None
    match_project: Union[MatchProject, None] = None
    change_project: Union[str, None] = None
    add_additional_info: Union[dict, None] = None


class Condition(BaseModel):
    key: str
    value: str
    operator: str


class Options(BaseModel):
    stop_processing: bool = False


class State(str, Enum):
    all = "ALL"
    any = "ANY"
    always = "ALWAYS"


class DataSourceRule(BaseModel):
    name: str
    conditions: List[Condition] = []
    conditions_policy: State
    actions: Actions
    options: Options = {}
    tags: dict = {}


class SupportedSecretType(str, Enum):
    manual = "MANUAL"
    use_service_account_secret = "USE_SERVICE_ACCOUNT_SECRET"


class PluginMetadata(BaseModel):
    data_source_rules: List[DataSourceRule] = []
    supported_secret_types: List[SupportedSecretType] = [SupportedSecretType.manual]
    currency: str = "USD"
    alias: dict = {}
    use_account_routing: bool = False
    account_match_key: Union[str, None] = None
    account_connect_polices: List[dict] = []
    additional_info: Dict[str, dict] = None


class PluginResponse(BaseModel):
    metadata: PluginMetadata

    class Config:
        use_enum_values = True
