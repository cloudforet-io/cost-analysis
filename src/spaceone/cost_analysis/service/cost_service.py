import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.core import cache
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.cost_model import AggregatedCost
from spaceone.cost_analysis.manager.cost_manager import CostManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager('CostManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['original_cost', 'original_currency', 'data_source_id', 'domain_id'])
    @change_timestamp_value(['billed_at'], timestamp_format='iso8601')
    def create(self, params):
        """Register cost

        Args:
            params (dict): {
                'original_cost': 'float',
                'original_currency': 'str',
                'usage_quantity': 'float',
                'provider': 'str',
                'region_code': 'str',
                'product': 'str',
                'account': 'str',
                'usage_type': 'str',
                'resource_group': 'str',
                'resource': 'str',
                'tags': 'dict',
                'additional_info': 'dict',
                'service_account_id': 'str',
                'project_id': 'str',
                'data_source_id': 'str',
                'billed_at': 'datetime',
                'domain_id': 'str'
            }

        Returns:
            cost_vo (object)
        """

        # validation check (service_account_id / project_id / data_source_id)

        return self.cost_mgr.create_cost(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cost_id', 'domain_id'])
    def delete(self, params):
        """Deregister cost

        Args:
            params (dict): {
                'cost_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.cost_mgr.delete_cost(params['cost_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cost_id', 'domain_id'])
    def get(self, params):
        """ Get cost

        Args:
            params (dict): {
                'cost_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            cost_vo (object)
        """

        cost_id = params['cost_id']
        domain_id = params['domain_id']

        return self.cost_mgr.get_cost(cost_id, domain_id, params.get('only'))

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['domain_id'])
    @append_query_filter(['cost_id', 'cost_key', 'original_currency', 'provider', 'region_code', 'region_key',
                          'product', 'account', 'usage_type', 'resource_group', 'resource', 'service_account_id',
                          'project_id', 'data_source_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['cost_id'])
    def list(self, params):
        """ List costs

        Args:
            params (dict): {
                'cost_id': 'str',
                'cost_key': 'str',
                'original_currency': 'str',
                'provider': 'str',
                'region_code': 'str',
                'region_key': 'str',
                'product': 'str',
                'account': 'str',
                'usage_type': 'str',
                'resource_group': 'str',
                'resource': 'str',
                'service_account_id': 'str',
                'project_id': 'str',
                'data_source_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta
            }

        Returns:
            cost_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.cost_mgr.list_costs(query)

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['cost_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params['domain_id']
        query = params.get('query', {})
        query_hash = utils.dict_to_hash(query)

        return self._stat_costs(query, query_hash, domain_id)

    @cache.cacheable(key='stat-costs:{domain_id}:{query_hash}', expire=3600 * 6)
    def _stat_costs(self, query, query_hash, domain_id):

        if self._is_raw_cost_target(query):
            return self.cost_mgr.stat_costs(query)
        else:
            _LOGGER.debug('[stat] stat_aggregated_costs')
            return self.cost_mgr.stat_aggregated_costs(query)

    @staticmethod
    def _is_raw_cost_target(query):
        aggregated_cost_fields = AggregatedCost.get_fields()
        keyword = query.get('keyword')
        distinct = query.get('distinct')
        aggregate = query.get('aggregate', [])
        _filter = query.get('filter', [])
        _filter_or = query.get('filter_or', [])

        if keyword:
            _LOGGER.debug('[stat] stat_costs: keyword')
            return True

        if distinct and distinct not in aggregated_cost_fields:
            _LOGGER.debug(f'[stat] stat_costs: distinct.{distinct}')
            return True

        for stage in aggregate:
            if 'group' in stage:
                keys = stage['group'].get('keys', []) + stage['group'].get('fields', [])

                for condition in keys:
                    key = condition.get('key')
                    if key and key not in aggregated_cost_fields:
                        _LOGGER.debug(f'[stat] stat_costs: aggregate.group.[keys|fields].{key}')
                        return True

        for condition in _filter:
            key = condition.get('k', condition.get('key'))
            if key not in aggregated_cost_fields:
                _LOGGER.debug(f'[stat] stat_costs: filter.{key}')
                return True

        for condition in _filter_or:
            key = condition.get('k', condition.get('key'))
            if key not in aggregated_cost_fields:
                _LOGGER.debug(f'[stat] stat_costs: filter_or.{key}')
                return True

        return False
