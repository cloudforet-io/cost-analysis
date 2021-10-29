import logging
import functools
from typing import List

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.data_source_rule_model import DataSourceRule, DataSourceRuleCondition

_LOGGER = logging.getLogger(__name__)


class DataSourceRuleManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_rule_model: DataSourceRule = self.locator.get_model('DataSourceRule')

    def create_data_source_rule(self, params):
        def _rollback(data_source_rule_vo: DataSourceRule):
            _LOGGER.info(f'[create_data_source_rule._rollback] '
                         f'Delete event rule : {data_source_rule_vo.name} '
                         f'({data_source_rule_vo.data_source_rule_id})')
            data_source_rule_vo.delete()

        data_source_rule_vo: DataSourceRule = self.data_source_rule_model.create(params)
        self.transaction.add_rollback(_rollback, data_source_rule_vo)

        return data_source_rule_vo

    def update_data_source_rule(self, params):
        data_source_rule_vo: DataSourceRule = self.get_data_source_rule(params['data_source_rule_id'],
                                                                        params['domain_id'])
        return self.update_data_source_rule_by_vo(params, data_source_rule_vo)

    def update_data_source_rule_by_vo(self, params, data_source_rule_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_data_source_rule_by_vo._rollback] Revert Data : '
                         f'{old_data["data_source_rule_id"]}')
            data_source_rule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_source_rule_vo.to_dict())

        return data_source_rule_vo.update(params)

    def delete_data_source_rule(self, data_source_rule_id, domain_id):
        data_source_rule_vo: DataSourceRule = self.get_data_source_rule(data_source_rule_id, domain_id)
        self.delete_data_source_rule_by_vo(data_source_rule_vo)

    def delete_data_source_rule_by_vo(self, data_source_rule_vo):
        data_source_rule_vo.delete()

    def get_data_source_rule(self, data_source_rule_id, domain_id, only=None):
        return self.data_source_rule_model.get(data_source_rule_id=data_source_rule_id, domain_id=domain_id, only=only)

    def list_data_source_rules(self, query={}):
        return self.data_source_rule_model.query(**query)

    def stat_data_source_rules(self, query):
        return self.data_source_rule_model.stat(**query)
