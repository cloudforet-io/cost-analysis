import logging
import copy
from datetime import datetime
from dateutil.relativedelta import relativedelta

from spaceone.core import cache, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.cost_model import Cost, MonthlyCost, CostQueryHistory
from spaceone.cost_analysis.manager.data_source_rule_manager import DataSourceRuleManager

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

        billed_at = self._get_billed_at_from_billed_date(params['billed_date'])

        params['billed_year'] = billed_at.strftime('%Y')
        params['billed_month'] = billed_at.strftime('%Y-%m')

        params = self.data_source_rule_mgr.change_cost_data(params)

        cost_vo: Cost = self.cost_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cost_vo)

        return cost_vo

    def create_monthly_cost(self, params):
        return self.monthly_cost_model.create(params)

    def delete_cost(self, cost_id, domain_id):
        cost_vo: Cost = self.get_cost(cost_id, domain_id)
        self.delete_cost_by_vo(cost_vo)

    @staticmethod
    def delete_cost_by_vo(cost_vo: Cost):
        cost_vo.delete()

    def delete_cost_with_datasource(self, domain_id, data_source_id):
        _LOGGER.debug(f'[delete_cost_with_datasource] data_source_id: {data_source_id}')
        cost_vos = self.cost_model.filter(domain_id=domain_id, data_source_id=data_source_id)
        cost_vos.delete()

        monthly_cost_vos = self.monthly_cost_model.filter(domain_id=domain_id, data_source_id=data_source_id)
        monthly_cost_vos.delete()

    def get_cost(self, cost_id, domain_id, only=None):
        return self.cost_model.get(cost_id=cost_id, domain_id=domain_id, only=only)

    def filter_costs(self, **conditions):
        return self.cost_model.filter(**conditions)

    def list_costs(self, query={}):
        return self.cost_model.query(**query)

    def stat_costs(self, query):
        return self.cost_model.stat(**query)

    def analyze_costs(self, query):
        return self.cost_model.analyze(**query)

    def filter_monthly_costs(self, **conditions):
        return self.monthly_cost_model.filter(**conditions)

    def list_monthly_costs(self, query={}):
        return self.monthly_cost_model.query(**query)

    def stat_monthly_costs(self, query):
        return self.monthly_cost_model.stat(**query)

    def analyze_monthly_costs(self, query):
        return self.monthly_cost_model.analyze(**query)

    @cache.cacheable(key='stat-costs:monthly:{domain_id}:{domain_id}:{query_hash}', expire=3600 * 24)
    def stat_monthly_costs_with_cache(self, query, query_hash, domain_id, data_source_id):
        return self.stat_monthly_costs(query)

    @cache.cacheable(key='analyze-costs:daily:{domain_id}:{data_source_id}:{query_hash}', expire=3600 * 24)
    def analyze_costs_with_cache(self, query, query_hash, domain_id, data_source_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        query['date_field'] = 'billed_date'
        return self.cost_model.analyze(**query)

    @cache.cacheable(key='analyze-costs:monthly:{domain_id}:{data_source_id}:{query_hash}', expire=3600 * 24)
    def analyze_monthly_costs_with_cache(self, query, query_hash, domain_id, data_source_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        query['date_field'] = 'billed_month'
        return self.monthly_cost_model.analyze(**query)

    @cache.cacheable(key='analyze-costs:yearly:{domain_id}:{data_source_id}:{query_hash}', expire=3600 * 24)
    def analyze_yearly_costs_with_cache(self, query, query_hash, domain_id, data_source_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        query['date_field'] = 'billed_year'
        return self.monthly_cost_model.analyze(**query)

    def analyze_costs_by_granularity(self, query, domain_id, data_source_id):
        self._check_date_range(query)
        granularity = query['granularity']
        # Save query history to speed up data loading
        query_hash = utils.dict_to_hash(query)
        self.create_cost_query_history(query, query_hash, domain_id, data_source_id)

        if granularity == 'DAILY':
            return self.analyze_costs_with_cache(query, query_hash, domain_id, data_source_id)
        elif granularity == 'MONTHLY':
            return self.analyze_monthly_costs_with_cache(query, query_hash, domain_id, data_source_id)
        elif granularity == 'YEARLY':
            return self.analyze_yearly_costs_with_cache(query, query_hash, domain_id, data_source_id)

    @cache.cacheable(key='cost-query-history:{domain_id}:{data_source_id}:{query_hash}', expire=600)
    def create_cost_query_history(self, query, query_hash, domain_id, data_source_id):
        def _rollback(history_vo):
            _LOGGER.info(f'[create_cost_query_history._rollback] Delete cost query history: {query_hash}')
            history_vo.delete()

        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_vo = history_model.create({
                'query_hash': query_hash,
                'query_options': copy.deepcopy(query),
                'data_source_id': data_source_id,
                'domain_id': domain_id
            })

            self.transaction.add_rollback(_rollback, history_vo)
        else:
            history_vos[0].update({})

    def list_cost_query_history(self, query={}):
        history_model: CostQueryHistory = self.locator.get_model('CostQueryHistory')
        return history_model.query(**query)

    @staticmethod
    def remove_stat_cache(domain_id, data_source_id):
        cache.delete_pattern(f'analyze-costs:*:{domain_id}:{data_source_id}:*')
        cache.delete_pattern(f'stat-costs:*:{domain_id}:{data_source_id}:*')
        cache.delete_pattern(f'cost-query-history:{domain_id}:{data_source_id}:*')

    def _check_date_range(self, query):
        start_str = query.get('start')
        end_str = query.get('end')
        granularity = query.get('granularity')

        start = self._parse_start_time(start_str, granularity)
        end = self._parse_end_time(end_str, granularity)
        now = datetime.utcnow().date()

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                           reason='End date must be greater than start date.')

        if granularity == 'DAILY':
            if start + relativedelta(months=1) < end:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='Request up to a maximum of 1 month.')

            if start + relativedelta(months=12) < now:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='For DAILY, you cannot request data older than 1 year.')

        elif granularity == 'MONTHLY':
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='Request up to a maximum of 12 months.')

            if start + relativedelta(months=36) < now:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='For MONTHLY, you cannot request data older than 3 years.')
        elif granularity == 'YEARLY':
            if start + relativedelta(years=3) < now:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='For YEARLY, you cannot request data older than 3 years.')

    def _parse_start_time(self, date_str, granularity):
        return self._convert_date_from_string(date_str.strip(), 'start', granularity)

    def _parse_end_time(self, date_str, granularity):
        end = self._convert_date_from_string(date_str.strip(), 'end', granularity)

        if granularity == 'YEARLY':
            return end + relativedelta(years=1)
        else:
            return end + relativedelta(months=1)

    @staticmethod
    def _convert_date_from_string(date_str, key, granularity):
        if granularity == 'YEARLY':
            try:
                return datetime.strptime(date_str, '%Y').date()
            except Exception as e:
                raise ERROR_INVALID_PARAMETER_TYPE(key=key, type='YYYY')
        else:
            try:
                return datetime.strptime(date_str, '%Y-%m').date()
            except Exception as e:
                raise ERROR_INVALID_PARAMETER_TYPE(key=key, type='YYYY-MM')

    @staticmethod
    def _get_billed_at_from_billed_date(billed_date):
        date_format = '%Y-%m-%d'

        try:
            return datetime.strptime(billed_date, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key='billed_date', type='YYYY-MM-DD')
