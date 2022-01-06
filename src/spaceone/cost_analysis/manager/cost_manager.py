import logging
import copy

from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.cost_model import Cost, CostQueryHistory
from spaceone.cost_analysis.manager.data_source_rule_manager import DataSourceRuleManager

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_model: Cost = self.locator.get_model('Cost')
        self.data_source_rule_mgr: DataSourceRuleManager = self.locator.get_manager('DataSourceRuleManager')

    def create_cost(self, params, execute_rollback=True):
        def _rollback(cost_vo):
            _LOGGER.info(f'[create_cost._rollback] '
                         f'Delete cost : {cost_vo.name} '
                         f'({cost_vo.cost_id})')
            cost_vo.delete()

        if 'region_code' in params and 'provider' in params:
            params['region_key'] = f'{params["provider"]}.{params["region_code"]}'

        if 'usd_cost' not in params:
            # check original currency
            # exchange rate applied to usd cost

            params['usd_cost'] = params['original_cost']

        params = self.data_source_rule_mgr.change_cost_data(params)

        cost_vo: Cost = self.cost_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cost_vo)

        return cost_vo

    def delete_cost(self, cost_id, domain_id):
        cost_vo: Cost = self.get_cost(cost_id, domain_id)
        cost_vo.delete()

    def get_cost(self, cost_id, domain_id, only=None):
        return self.cost_model.get(cost_id=cost_id, domain_id=domain_id, only=only)

    def filter_costs(self, **conditions):
        return self.cost_model.filter(**conditions)

    def list_costs(self, query={}):
        return self.cost_model.query(**query)

    def stat_costs(self, query):
        return self.cost_model.stat(**query)

    def filter_cost_query_history(self, **conditions):
        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')
        return history_model.filter(**conditions)

    @cache.cacheable(key='stat-costs-history:{domain_id}:{query_hash}', expire=600)
    def create_cost_query_history(self, query, query_hash, start, end, domain_id):
        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_model.create({
                'query_hash': query_hash,
                'query': copy.deepcopy(query),
                'start': start,
                'end': end,
                'domain_id': domain_id
            })
        else:
            history_vos[0].update({
                'start': start,
                'end': end
            })

        return True

    @cache.cacheable(key='stat-costs:{domain_id}:{query_hash}', expire=3600 * 24)
    def stat_costs_with_cache(self, query, query_hash, domain_id):
        return self.stat_costs(query)

    @staticmethod
    def remove_stat_cache(domain_id):
        cache.delete_pattern(f'stat-costs:{domain_id}:*')
