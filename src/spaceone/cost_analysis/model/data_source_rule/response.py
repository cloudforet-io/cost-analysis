from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

from spaceone.cost_analysis.model.data_source.request import ResourceGroup
from spaceone.cost_analysis.model.data_source_rule.request import RuleType, ConditionPolicy

__all__ = [
    "DataSourceRuleResponse",
    "DataSourceRulesResponse",
]


class DataSourceRuleResponse(BaseModel):
    data_source_rule_id: Union[str, None] = None
    name: Union[str, None] = None
    rule_type: Union[RuleType, None] = None
    order: Union[int, None] = None
    conditions: Union[list, None] = None
    conditions_policy: Union[ConditionPolicy, None] = None
    actions: Union[dict, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    data_source_id: Union[str, None] = None
    data_source: Union[dict, None] = None
    resource_group: Union[ResourceGroup, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        return data


class DataSourceRulesResponse(BaseModel):
    results: List[DataSourceRuleResponse]
    total_count: int