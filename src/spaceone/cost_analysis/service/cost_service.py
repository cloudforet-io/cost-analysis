import logging
from datetime import datetime

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager

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
                'category': 'str',
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

        params['billed_at'] = params.get('billed_at') or datetime.utcnow()

        cost_vo = self.cost_mgr.create_cost(params)

        self.cost_mgr.remove_stat_cache(params['domain_id'])

        return cost_vo

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

        self.cost_mgr.remove_stat_cache(params['domain_id'])

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
    @change_timestamp_value(['start', 'end'], timestamp_format='iso8601')
    @append_query_filter(['cost_id', 'original_currency', 'provider', 'region_code', 'region_key', 'category',
                          'product', 'account', 'usage_type', 'resource_group', 'resource', 'service_account_id',
                          'project_id', 'data_source_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['cost_id'])
    def list(self, params):
        """ List costs

        Args:
            params (dict): {
                'cost_id': 'str',
                'original_currency': 'str',
                'provider': 'str',
                'region_code': 'str',
                'region_key': 'str',
                'category': 'str',
                'product': 'str',
                'account': 'str',
                'usage_type': 'str',
                'resource_group': 'str',
                'resource': 'str',
                'start': 'datetime',
                'end': 'datetime',
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

        start = params.get('start')
        end = params.get('end')
        query = params.get('query', {})
        query = self._change_project_group_filter(query, params['domain_id'])
        query = self._add_date_range_filter(query, start, end)

        return self.cost_mgr.list_costs(query)

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['query', 'domain_id'])
    @change_timestamp_value(['start', 'end'], timestamp_format='iso8601')
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['cost_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'start': 'datetime',
                'end': 'datetime',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params['domain_id']
        start = params.get('start')
        end = params.get('end')
        query = params.get('query', {})
        query = self._change_project_group_filter(query, params['domain_id'])
        query_hash = utils.dict_to_hash(query)

        # Save query for improve performance
        self.cost_mgr.create_cost_query_history(query, query_hash, start, end, domain_id)

        query = self._add_date_range_filter(query, start, end)
        query_hash_with_date_range = utils.dict_to_hash(query)

        return self.cost_mgr.stat_costs_with_cache(query, query_hash_with_date_range, domain_id)

    @staticmethod
    def _add_date_range_filter(query, start, end):
        query['filter'] = query.get('filter') or []

        if start:
            query['filter'].append({
                'k': 'billed_at',
                'v': utils.datetime_to_iso8601(start),
                'o': 'datetime_gte'
            })

        if end:
            query['filter'].append({
                'k': 'billed_at',
                'v': utils.datetime_to_iso8601(end),
                'o': 'datetime_lt'
            })

        return query

    def _change_project_group_filter(self, query, domain_id):
        changed_filter = []
        changed_filter_or = []
        project_group_query = {
            'filter': [],
            'filter_or': [],
            'only': ['project_group_id']
        }

        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if not all([key, value, operator]):
                raise ERROR_DB_QUERY(reason='Filter condition should have key, value and operator.')

            if key == 'project_group_id':
                project_group_query['filter'].append(condition)
            else:
                changed_filter.append(condition)

        for condition in query.get('filter_or', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if not all([key, value, operator]):
                raise ERROR_DB_QUERY(reason='FilterOr condition should have key, value and operator.')

            if key == 'project_group_id':
                project_group_query['filter_or'].append(condition)
            else:
                changed_filter_or.append(condition)

        if len(project_group_query['filter']) > 0 or len(project_group_query['filter_or']) > 0:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            response = identity_mgr.list_project_groups(project_group_query, domain_id)
            project_group_ids = []
            project_ids = []
            for project_group_info in response.get('results', []):
                project_group_ids.append(project_group_info['project_group_id'])

            for project_group_id in project_group_ids:
                response = identity_mgr.list_projects_in_project_group(project_group_id, domain_id, True,
                                                                       {
                                                                           'only': ['project_id']
                                                                       })
                for project_info in response.get('results', []):
                    if project_info['project_id'] not in project_ids:
                        project_ids.append(project_info['project_id'])

            changed_filter.append({'k': 'project_id', 'v': project_ids, 'o': 'in'})

        query['filter'] = changed_filter
        query['filter_or'] = changed_filter_or

        return query
