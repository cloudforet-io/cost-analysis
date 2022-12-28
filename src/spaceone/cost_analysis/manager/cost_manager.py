import logging
import copy
import pandas as pd
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from spaceone.core import cache, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.cost_model import Cost, MonthlyCost, CostQueryHistory
from spaceone.cost_analysis.manager.data_source_rule_manager import DataSourceRuleManager
from spaceone.cost_analysis.manager.exchange_rate_manager import ExchangeRateManager

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_model: Cost = self.locator.get_model('Cost')
        self.monthly_cost_model: MonthlyCost = self.locator.get_model('MonthlyCost')
        self.data_source_rule_mgr: DataSourceRuleManager = self.locator.get_manager('DataSourceRuleManager')
        self.exchange_rate_map = None

    def create_cost(self, params, execute_rollback=True):
        def _rollback(cost_vo):
            _LOGGER.info(f'[create_cost._rollback] '
                         f'Delete cost : {cost_vo.name} '
                         f'({cost_vo.cost_id})')
            cost_vo.delete()

        if 'region_code' in params and 'provider' in params:
            params['region_key'] = f'{params["provider"]}.{params["region_code"]}'

        if 'usd_cost' not in params:
            original_currency = params['original_currency']
            original_cost = params['original_cost']

            if original_currency == 'USD':
                params['usd_cost'] = params['original_cost']
            else:
                self._load_exchange_rate_map(params['domain_id'])

                rate = self.exchange_rate_map.get(original_currency)

                if rate is None:
                    raise ERROR_UNSUPPORTED_CURRENCY(supported_currency=list(self.exchange_rate_map.keys()))

                params['usd_cost'] = round(original_cost / rate, 15)

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

    def analyze_costs(self, query, domain_id):
        granularity = query['granularity']
        start, end = self._parse_date_range(query, granularity)
        page_limit = query.get('page', {}).get('limit')

        self._create_cost_query_history_v2(query, granularity, start, end, domain_id)
        stat_query = self._make_stat_query(query, granularity, start, end)
        query_hash = utils.dict_to_hash(stat_query)

        if self.is_monthly_cost(granularity, start, end):
            response = self.stat_monthly_costs_with_cache(stat_query, query_hash, domain_id)
        else:
            response = self.stat_costs_with_cache(stat_query, query_hash, domain_id)

        if page_limit:
            response['more'] = len(response['results']) > page_limit
            response['results'] = response['results'][:page_limit]

        return response

    def _create_cost_query_history_v2(self, query, granularity, start, end, domain_id):
        analyze_query = {
            'group_by': query.get('group_by'),
            'field_group': query.get('field_group'),
            'fields': query.get('fields'),
            'sort': query.get('sort'),
            'page': query.get('page'),
        }
        query_hash = utils.dict_to_hash(analyze_query)
        self.create_cost_query_history(domain_id, analyze_query, query_hash, granularity, start, end)

    def _make_stat_query(self, query, granularity, start, end):
        group_by = query.get('group_by') or []
        fields = query.get('fields') or []
        field_group = query.get('field_group') or []
        self._check_field_group(field_group)
        has_field_group = len(field_group) > 0
        sort = query.get('sort') or []
        page = query.get('page')

        group_keys = self._make_group_keys(group_by, granularity)
        group_fields = self._make_group_fields(fields)

        stat_query = {
            'filter': query.get('filter', []),
            'filter_or': query.get('filter_or', []),
            'aggregate': [
                {
                    'group': {
                        'keys': group_keys,
                        'fields': group_fields
                    }
                }
            ]
        }

        if has_field_group > 0:
            stat_query['aggregate'] += self._make_field_group_query(group_keys, group_fields, field_group)

        if len(sort) > 0:
            stat_query['aggregate'] += self._make_sort_query(sort, group_fields, has_field_group)

        if page:
            stat_query['aggregate'] += self._make_page_query(page)

        stat_query = self.add_date_range_filter(stat_query, granularity, start, end)
        return stat_query

    def _make_page_query(self, page):
        page_query = []
        start = page.get('start')
        limit = page.get('limit')

        if limit:
            if start:
                page_query.append({
                    'skip': start - 1
                })

            page_query.append({
                'limit': limit + 1
            })

        return page_query

    def _make_sort_query(self, sort, group_fields, has_field_group):
        sort_query = {
            'sort': {
                'keys': []
            }
        }
        if has_field_group:
            group_field_names = []
            for group_field in group_fields:
                group_field_names.append(group_field['name'])

            for condition in sort:
                key = condition.get('key')
                desc = condition.get('desc', False)

                if key in group_field_names:
                    key = f'_total_{key}'

                sort_query['sort']['keys'].append({
                    'key': key,
                    'desc': desc
                })
        else:
            sort_query['sort']['keys'] = sort

        return [sort_query]

    def _check_field_group(self, field_group):
        for field_group_key in field_group:
            if field_group_key.startswith('_total_'):
                raise ERROR_INVALID_PARAMETER(key='field_group',
                                              reason='Field group keys cannot contain _total_ characters.')

    def _make_field_group_query(self, group_keys, group_fields, field_group):
        field_group_query = {
            'group': {
                'keys': self._make_field_group_keys(group_keys, field_group),
                'fields': self._make_field_group_fields(group_fields, field_group)
            }
        }

        return [field_group_query]

    def _make_field_group_keys(self, group_keys, field_group):
        field_group_keys = []
        for group_key in group_keys:
            if group_key['name'] not in field_group:
                if group_key['name'] == 'date':
                    field_group_keys.append({
                        'key': 'date',
                        'name': 'date'
                    })
                else:
                    field_group_keys.append(group_key)

        return field_group_keys

    def _make_field_group_fields(self, group_fields, field_group):
        field_group_fields = []
        for group_field in group_fields:
            field_name = group_field['name']
            operator = group_field['operator']

            if operator in ['sum', 'average', 'max', 'min', 'count']:
                field_group_fields.append({
                    'key': field_name,
                    'name': f'_total_{field_name}',
                    'operator': 'sum' if operator == 'count' else operator
                })

            field_group_fields.append({
                'name': field_name,
                'operator': 'push',
                'fields': self._make_field_group_push_fields(field_name, field_group)
            })

        return field_group_fields

    def _make_field_group_push_fields(self, field_name, field_group):
        push_fields = [
            {
                'name': 'value',
                'key': field_name
            }
        ]

        for field_group_key in field_group:
            push_fields.append({
                'name': field_group_key,
                'key': field_group_key
            })

        return push_fields

    def _make_group_fields(self, fields):
        group_fields = []
        for name, field_info in fields.items():
            self._check_field_info(field_info)
            operator = field_info['operator']
            key = field_info.get('key')
            fields = field_info.get('fields')

            group_field = {
                'name': name,
                'operator': operator
            }

            if operator != 'count':
                group_field['key'] = key

            if operator == 'push':
                group_field['fields'] = fields

            group_fields.append(group_field)

        return group_fields

    def _check_field_info(self, field_info):
        key = field_info.get('key')
        operator = field_info.get('operator')
        fields = field_info.get('fields', [])

        if operator is None:
            raise ERROR_REQUIRED_PARAMETER(key='query.fields.operator')

        # Check Operator

        if operator != 'count' and key is None:
            raise ERROR_REQUIRED_PARAMETER(key='query.fields.key')

        if operator == 'push' and len(fields) == 0:
            raise ERROR_REQUIRED_PARAMETER(key='query.fields.fields')

    def _make_group_keys(self, group_by, granularity):
        group_keys = []
        for key in group_by:
            group_keys.append({
                'key': key,
                'name': key
            })

        if granularity == 'DAILY':
            group_keys.append({
                'key': 'billed_date',
                'name': 'date'
            })
        elif granularity == 'MONTHLY':
            group_keys.append({
                'key': 'billed_month',
                'name': 'date'
            })

        return group_keys

    def _parse_date_range(self, query, granularity):
        start_str = query.get('start')
        end_str = query.get('end')

        if start_str is None:
            raise ERROR_REQUIRED_PARAMETER(key='query.start')

        if end_str is None:
            raise ERROR_REQUIRED_PARAMETER(key='query.End')

        start = self._parse_start_time(start_str)
        end = self._parse_end_time(end_str)

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(reason='End date must be greater than start date.')

        if granularity in ['ACCUMULATED', 'MONTHLY']:
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(reason='Request up to a maximum of 12 months.')
        elif granularity == 'DAILY':
            if start + relativedelta(days=31) < end:
                raise ERROR_INVALID_DATE_RANGE(reason='Request up to a maximum of 31 days.')

        return start, end

    def _parse_start_time(self, date_str):
        return self._convert_date_from_string(date_str.strip(), 'start')

    def _parse_end_time(self, date_str):
        date = self._convert_date_from_string(date_str.strip(), 'end')

        if len(date_str.strip()) == 7:
            return date + relativedelta(months=1)
        else:
            return date + relativedelta(days=1)

    @staticmethod
    def _convert_date_from_string(date_str, key):
        if len(date_str) == 7:
            # Month (YYYY-MM)
            date_format = '%Y-%m'
        else:
            # Date (YYYY-MM-DD)
            date_format = '%Y-%m-%d'

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_format)

    def list_cost_query_history(self, query={}):
        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')
        return history_model.query(**query)

    @cache.cacheable(key='stat-costs-history:{domain_id}:{query_hash}', expire=600)
    def create_cost_query_history(self, domain_id, query, query_hash, granularity=None, start=None, end=None):
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
    def stat_costs_with_cache(self, query, query_hash, domain_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        return self.stat_costs(query)

    @cache.cacheable(key='stat-monthly-costs:{domain_id}:{query_hash}', expire=3600 * 24)
    def stat_monthly_costs_with_cache(self, query, query_hash, domain_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
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

    def add_date_range_filter(self, query, granularity, start: date, end: date):
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

    def make_accumulated_query(self, group_by, limit, query_filter, include_others=False,
                               include_usage_quantity=False, has_project_group_id=False):
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

        if include_usage_quantity:
            aggregate[0]['group']['fields'].append({
                'key': 'usage_quantity',
                'name': 'usage_quantity',
                'operator': 'sum'
            })

        if limit and include_others is False and has_project_group_id is False:
            aggregate.append({'limit': limit})

        return {
            'aggregate': aggregate,
            'filter': query_filter
        }

    def make_trend_query(self, granularity, group_by, limit, query_filter, include_others=False,
                         include_usage_quantity=False, has_project_group_id=False):
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
                    'keys': self._get_keys_from_group_by(group_by, change_key=True),
                    'fields': [
                        {
                            'key': 'usd_cost',
                            'name': 'total_usd_cost',
                            'operator': 'sum'
                        },
                        {
                            'name': 'usd_cost_values',
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

        if include_usage_quantity:
            aggregate[0]['group']['fields'].append({
                'key': 'usage_quantity',
                'name': 'usage_quantity',
                'operator': 'sum'
            })
            aggregate[1]['group']['fields'] += [
                {
                    'key': 'usage_quantity',
                    'name': 'total_usage_quantity',
                    'operator': 'sum'
                },
                {
                    'name': 'usage_quantity_values',
                    'operator': 'push',
                    'fields': [
                        {
                            'key': 'date',
                            'name': 'k'
                        },
                        {
                            'key': 'usage_quantity',
                            'name': 'v'
                        }
                    ]
                }
            ]

        if limit and include_others is False and has_project_group_id is False:
            aggregate.append({'limit': limit})

        aggregate.append({
            'project': {
                'fields': [
                    {
                        'key': 'total_usd_cost',
                        'name': 'total_usd_cost'
                    },
                    {
                        'key': 'usd_cost_values',
                        'name': 'usd_cost',
                        'operator': 'array_to_object'
                    }
                ]
            }
        })

        if include_usage_quantity:
            aggregate[-1]['project']['fields'] += [
                {
                    'key': 'total_usage_quantity',
                    'name': 'total_usage_quantity'
                },
                {
                    'key': 'usage_quantity_values',
                    'name': 'usage_quantity',
                    'operator': 'array_to_object'
                }
            ]

        return {
            'aggregate': aggregate,
            'filter': query_filter
        }

    def sum_costs_by_project_group(self, response, granularity, group_by, domain_id, include_usage_quantity=False):
        has_project_id = 'project_id' in group_by
        cost_keys = list(set(group_by[:] + ['project_id', 'usd_cost']))

        if include_usage_quantity:
            cost_keys.append('usage_quantity')

        if granularity != 'ACCUMULATED':
            cost_keys.append('total_usd_cost')

            if include_usage_quantity:
                cost_keys.append('total_usage_quantity')

        cost_keys.remove('project_group_id')
        results = response.get('results', [])

        _LOGGER.debug(f'[sum_costs_by_project_group] cost_keys: {cost_keys}')
        projects_info = self._get_projects_info(domain_id)
        project_df = pd.DataFrame(projects_info, columns=['project_id', 'project_group_id'])
        cost_df = pd.DataFrame(results, columns=cost_keys)
        join_df = pd.merge(cost_df, project_df, on=['project_id'], how='left')

        if not has_project_id:
            del join_df['project_id']

        if granularity == 'ACCUMULATED':
            join_df = join_df.groupby(by=group_by, dropna=False, as_index=False).sum()
            if len(join_df) > 0:
                join_df = join_df.sort_values(by='usd_cost', ascending=False)
                join_df = join_df.replace({np.nan: None})

            return {
                'results': join_df.to_dict('records')
            }

        else:
            aggr_values = {
                'usd_cost': list,
                'total_usd_cost': sum
            }

            if include_usage_quantity:
                aggr_values['usage_quantity'] = list
                aggr_values['total_usage_quantity'] = sum

            join_df = join_df.groupby(by=group_by, dropna=False, as_index=False).agg(aggr_values)

            if len(join_df) > 0:
                join_df = join_df.sort_values(by='total_usd_cost', ascending=False)
                join_df = join_df.replace({np.nan: None})

            changed_results = []
            for cost_info in join_df.to_dict('records'):
                usd_cost_list = cost_info['usd_cost']
                changed_usd_cost = {}
                for usd_cost_info in usd_cost_list:
                    for key, usd_cost in usd_cost_info.items():
                        if key not in changed_usd_cost:
                            changed_usd_cost[key] = usd_cost
                        else:
                            changed_usd_cost[key] += usd_cost

                cost_info['usd_cost'] = changed_usd_cost

                if include_usage_quantity:
                    usage_quantity_list = cost_info['usage_quantity']
                    changed_usage_quantity = {}
                    for usage_quantity_info in usage_quantity_list:
                        for key, usage_quantity in usage_quantity_info.items():
                            if key not in changed_usage_quantity:
                                changed_usage_quantity[key] = usage_quantity
                            else:
                                changed_usage_quantity[key] += usage_quantity

                    cost_info['usage_quantity'] = changed_usage_quantity

                changed_results.append(cost_info)

            return {
                'results': changed_results
            }

    @staticmethod
    def sum_costs_over_limit(response, granularity, limit, include_usage_quantity=False):
        results = response.get('results', [])
        changed_results = results[:limit]

        if granularity == 'ACCUMULATED':
            others = {
                'is_others': True,
                'usd_cost': 0
            }

            if include_usage_quantity:
                others['usage_quantity'] = 0

            for cost_info in results[limit:]:
                others['usd_cost'] += cost_info['usd_cost']

                if include_usage_quantity:
                    others['usage_quantity'] += cost_info['usage_quantity']

        else:
            others = {
                'is_others': True,
                'total_usd_cost': 0,
                'usd_cost': {}
            }

            if include_usage_quantity:
                others['total_usage_quantity'] = 0
                others['usage_quantity'] = {}

            for cost_info in results[limit:]:
                others['total_usd_cost'] += cost_info['total_usd_cost']

                for key, usd_cost in cost_info['usd_cost'].items():
                    if key not in others['usd_cost']:
                        others['usd_cost'][key] = usd_cost
                    else:
                        others['usd_cost'][key] += usd_cost

                if include_usage_quantity:
                    others['total_usage_quantity'] += cost_info['total_usage_quantity']

                    for key, usage_quantity in cost_info['usage_quantity'].items():
                        if key not in others['usage_quantity']:
                            others['usage_quantity'][key] = usage_quantity
                        else:
                            others['usage_quantity'][key] += usage_quantity

        changed_results.append(others)

        return {
            'results': changed_results
        }

    @staticmethod
    def slice_results(response, limit):
        results = response.get('results', [])

        return {
            'results': results[:limit]
        }

    @staticmethod
    def page_results(response, page):
        results = response.get('results', [])
        response = {
            'total_count': len(results)
        }

        if 'limit' in page and page['limit'] > 0:
            start = page.get('start', 1)
            if start < 1:
                start = 1

            response['results'] = results[start - 1:start + page['limit'] - 1]
        else:
            response['results'] = results

        return response

    @staticmethod
    def _get_keys_from_group_by(group_by, change_key=False):
        keys = []
        group_keys = group_by[:]

        if 'project_group_id' in group_keys:
            if 'project_id' not in group_keys:
                group_keys.append('project_id')

            group_keys.remove('project_group_id')

        for key in group_keys:
            keys.append({
                'key': key.replace('.', '_') if change_key else key,
                'name': key.replace('.', '_')
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

    @cache.cacheable(key='domain-all-project-info:{domain_id}', expire=300)
    def _get_projects_info(self, domain_id):
        projects_info = []

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        query = {'only': ['project_id', 'project_group_id']}
        response = identity_mgr.list_projects(query, domain_id)

        for project_info in response.get('results', []):
            projects_info.append({
                'project_id': project_info['project_id'],
                'project_group_id': project_info['project_group_info']['project_group_id']
            })

        return projects_info

    def _load_exchange_rate_map(self, domain_id):
        if self.exchange_rate_map is None:
            self.exchange_rate_map = {}
            exchange_rate_mgr: ExchangeRateManager = self.locator.get_manager('ExchangeRateManager')
            results, total_count = exchange_rate_mgr.list_all_exchange_rates(domain_id)

            for exchange_rate_data in results:
                if exchange_rate_data.get('state', 'ENABLED') == 'ENABLED':
                    self.exchange_rate_map[exchange_rate_data['currency']] = exchange_rate_data['rate']

    @staticmethod
    def is_monthly_stat_query(query):
        _filter = query.get('filter', [])
        _aggregate = query.get('aggregate', [])

        is_monthly_query = False

        if len(_aggregate) > 0 and len(_filter) > 0:
            has_date_key = False
            has_month_start = False
            has_month_end = False

            stage = _aggregate[0]
            if 'group' in stage:
                keys = stage['group'].get('keys', [])
                for options in keys:
                    key = options.get('key', options.get('k'))
                    if key == 'billed_date' or key == 'billed_at':
                        has_date_key = True
                        break

            for condition in _filter:
                key = condition.get('key', condition.get('k'))
                operator = condition.get('operator', condition.get('o'))

                if key == 'billed_month':
                    if operator in ['gt', 'gte']:
                        has_month_start = True
                    elif operator in ['lt', 'lte']:
                        has_month_end = True

            if has_date_key is False and has_month_start and has_month_end:
                is_monthly_query = True

        return is_monthly_query
