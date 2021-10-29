import functools
from typing import List
from spaceone.api.cost_analysis.v1 import data_source_rule_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

from spaceone.cost_analysis.model.data_source_rule_model import DataSourceRule, DataSourceRuleCondition, DataSourceRuleOptions

__all__ = ['DataSourceRuleInfo', 'DataSourceRulesInfo']


def DataSourceRuleConditionsInfo(condition_vos: List[DataSourceRuleCondition]):
    if condition_vos is None:
        condition_vos = []

    conditions_info = []

    for vo in condition_vos:
        info = {
            'key': vo.key,
            'value': vo.value,
            'operator': vo.operator
        }

        conditions_info.append(data_source_rule_pb2.DataSourceRuleCondition(**info))

    return conditions_info


def DataSourceRuleActionMatchRuleInfo(match_rule_data):
    if match_rule_data is None:
        return None

    info = {
        'source': match_rule_data.get('source'),
        'target': match_rule_data.get('target')
    }

    return data_source_rule_pb2.MatchRule(**info)


def DataSourceRuleActionsInfo(actions_data):
    if actions_data is None:
        return None
    else:
        info = {}

        for key, value in actions_data.items():
            if key in ['match_project', 'match_service_account']:
                info[key] = DataSourceRuleActionMatchRuleInfo(value)
            elif key == 'add_additional_info':
                info[key] = change_struct_type(value)
            else:
                info[key] = value

        return data_source_rule_pb2.DataSourceRuleActions(**info)


def DataSourceRuleOptionsInfo(vo: DataSourceRuleOptions):
    if vo is None:
        return None
    else:
        info = {
            'stop_processing': vo.stop_processing
        }

        return data_source_rule_pb2.DataSourceRuleOptions(**info)


def DataSourceRuleInfo(data_source_rule_vo: DataSourceRule, minimal=False):
    info = {
        'data_source_rule_id': data_source_rule_vo.data_source_rule_id,
        'name': data_source_rule_vo.name,
        'order': data_source_rule_vo.order,
        'data_source_id': data_source_rule_vo.data_source_id,
    }

    if not minimal:
        info.update({
            'conditions': DataSourceRuleConditionsInfo(data_source_rule_vo.conditions),
            'conditions_policy': data_source_rule_vo.conditions_policy,
            'actions': DataSourceRuleActionsInfo(data_source_rule_vo.actions),
            'options': DataSourceRuleOptionsInfo(data_source_rule_vo.options),
            'tags': change_struct_type(data_source_rule_vo.tags),
            'domain_id': data_source_rule_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(data_source_rule_vo.created_at)
        })

    return data_source_rule_pb2.DataSourceRuleInfo(**info)


def DataSourceRulesInfo(data_source_rule_vos, total_count, **kwargs):
    return data_source_rule_pb2.DataSourceRulesInfo(results=list(
        map(functools.partial(DataSourceRuleInfo, **kwargs), data_source_rule_vos)), total_count=total_count)
