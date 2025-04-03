from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "DataSourceRuleCreateRequest",
    "DataSourceRuleUpdateRequest",
    "DataSourceRuleChangeOrderRequest",
    "DataSourceRuleDeleteRequest",
    "DataSourceRuleGetRequest",
    "DataSourceRuleSearchQueryRequest",
    "DataSourceRuleStatQueryRequest",
]

from spaceone.cost_analysis.model.data_source.request import ResourceGroup

RuleType = Literal["MANAGED", "CUSTOM"]
ConditionPolicy = Literal["ALL", "ANY", "ALWAYS"]


class DataSourceRuleCreateRequest(BaseModel):
    data_source_id: str
    name: Union[str, None] = None
    conditions: Union[list, None] = None
    conditions_policy: ConditionPolicy
    actions: dict
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    resource_group: ResourceGroup
    workspace_id: str
    domain_id: str


class DataSourceRuleUpdateRequest(BaseModel):
    data_source_rule_id: str
    name: Union[str, None] = None
    conditions: Union[list, None] = None
    conditions_policy: Union[ConditionPolicy, None] = None
    actions: Union[dict, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceRuleChangeOrderRequest(BaseModel):
    data_source_rule_id: str
    order: int
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceRuleDeleteRequest(BaseModel):
    data_source_rule_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceRuleGetRequest(BaseModel):
    data_source_rule_id: str
    workspace_id: str
    domain_id: str


class DataSourceRuleSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    data_source_rule_id: Union[str, None] = None
    name: Union[str, None] = None
    rule_type: Union[str, None] = None
    data_source_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class DataSourceRuleStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
