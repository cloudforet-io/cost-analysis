import logging
import copy
from datetime import datetime

from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.cost_model import Cost, MonthlyCost, CostQueryHistory
from spaceone.cost_analysis.manager.data_source_rule_manager import DataSourceRuleManager

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_model: Cost = self.locator.get_model('Cost')
        self.monthly_cost_model: MonthlyCost = self.locator.get_model('MonthlyCost')
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

        params['billed_year'] = params['billed_at'].strftime('%Y')
        params['billed_month'] = params['billed_at'].strftime('%Y-%m')
        params['billed_date'] = params['billed_at'].strftime('%Y-%m-%d')

        params = self.data_source_rule_mgr.change_cost_data(params)

        cost_vo: Cost = self.cost_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cost_vo)

        return cost_vo

    def create_monthly_cost(self, params):
        return self.monthly_cost_model.create(params)

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

    def list_monthly_costs(self, query={}):
        return self.monthly_cost_model.query(**query)

    def stat_monthly_costs(self, query):
        return self.monthly_cost_model.stat(**query)

    def list_cost_query_history(self, query={}):
        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')
        return history_model.query(**query)

    @cache.cacheable(key='stat-costs-history:{domain_id}:{query_hash}', expire=600)
    def create_cost_query_history(self, query, query_hash, granularity, start, end, domain_id):
        def _rollback(history_vo):
            _LOGGER.info(f'[create_cost_query_history._rollback] Delete cost query history: {query_hash}')
            history_vo.delete()

        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_vo = history_model.create({
                'query_hash': query_hash,
                'query_options': copy.deepcopy(query),
                'granularity': granularity,
                'start': start,
                'end': end,
                'domain_id': domain_id
            })

            self.transaction.add_rollback(_rollback, history_vo)
        else:
            history_vos[0].update({
                'start': start,
                'end': end
            })

    @cache.cacheable(key='stat-costs:{domain_id}:{query_hash}', expire=3600 * 24)
    def stat_costs_with_cache(self, query, query_hash, domain_id):
        return self.stat_costs(query)

    @cache.cacheable(key='stat-monthly-costs:{domain_id}:{query_hash}', expire=3600 * 24)
    def stat_monthly_costs_with_cache(self, query, query_hash, domain_id):
        return self.stat_monthly_costs(query)

    @staticmethod
    def remove_stat_cache(domain_id):
        cache.delete_pattern(f'stat-costs:{domain_id}:*')
        cache.delete_pattern(f'stat-monthly-costs:{domain_id}:*')

    @staticmethod
    def is_monthly_cost(granularity, start, end):
        if granularity in ['ACCUMULATED', 'MONTHLY'] and start.day == 1 and end.day == 1:
            return True
        else:
            return False

    def add_date_range_filter(self, query, granularity, start: datetime, end: datetime):
        query['filter'] = query.get('filter') or []

        if self.is_monthly_cost(granularity, start, end):
            query['filter'].append({
                'k': 'billed_month',
                'v': start.strftime('%Y-%m'),
                'o': 'gte'
            })

            query['filter'].append({
                'k': 'billed_month',
                'v': end.strftime('%Y-%m'),
                'o': 'lt'
            })

        else:
            query['filter'].append({
                'k': 'billed_date',
                'v': start.strftime('%Y-%m-%d'),
                'o': 'gte'
            })

            query['filter'].append({
                'k': 'billed_date',
                'v': end.strftime('%Y-%m-%d'),
                'o': 'lt'
            })

        return query

    def make_accumulated_query(self, granularity, group_by, limit, page, sort, query_filter):
        aggregate = [
            {
                'group': {
                    'keys': self._get_keys_from_group_by(group_by),
                    'fields': [
                        {
                            'key': 'usd_cost',
                            'name': 'usd_cost',
                            'operator': 'sum'
                        }
                    ]
                }
            },
            {
                'sort': {
                    'key': 'usd_cost',
                    'desc': True
                }
            }
        ]

        if limit:
            aggregate.append({'limit': limit})

        return {
            'aggregate': aggregate,
            'page': page,
            'filter': query_filter
        }

    def make_trend_query(self, granularity, group_by, limit, page, sort, query_filter):
        aggregate = [
            {
                'group': {
                    'keys': self._get_keys_from_group_by(group_by) + self._get_keys_from_granularity(granularity),
                    'fields': [
                        {
                            'key': 'usd_cost',
                            'name': 'usd_cost',
                            'operator': 'sum'
                        }
                    ]
                }
            },
            {
                'group': {
                    'keys': self._get_keys_from_group_by(group_by),
                    'fields': [
                        {
                            'key': 'usd_cost',
                            'name': 'total_usd_cost',
                            'operator': 'sum'
                        },
                        {
                            'name': 'values',
                            'operator': 'push',
                            'fields': [
                                {
                                    'key': 'date',
                                    'name': 'k'
                                },
                                {
                                    'key': 'usd_cost',
                                    'name': 'v'
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'sort': {
                    'key': 'total_usd_cost',
                    'desc': True
                }
            }
        ]

        if limit:
            aggregate.append({'limit': limit})

        aggregate.append({
            'project': {
                'fields': [
                    {
                        'key': 'values',
                        'name': 'usd_cost',
                        'operator': 'array_to_object'
                    }
                ]
            }
        })

        return {
            'aggregate': aggregate,
            'page': page,
            'filter': query_filter
        }

    @staticmethod
    def _get_keys_from_group_by(group_by):
        keys = []
        for key in group_by:
            keys.append({
                'key': key,
                'name': key
            })
        return keys

    @staticmethod
    def _get_keys_from_granularity(granularity):
        keys = []
        if granularity == 'DAILY':
            keys.append({
                'key': 'billed_date',
                'name': 'date'
            })
        elif granularity == 'MONTHLY':
            keys.append({
                'key': 'billed_month',
                'name': 'date'
            })
        elif granularity == 'YEARLY':
            keys.append({
                'key': 'billed_year',
                'name': 'date'
            })

        return keys
