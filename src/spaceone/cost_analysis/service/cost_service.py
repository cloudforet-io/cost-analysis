import logging
from datetime import datetime

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
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
    @check_required(['original_cost', 'original_currency', 'project_id', 'data_source_id', 'domain_id'])
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
                'type': 'str',
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

        params['billed_at'] = params.get('billed_at') or datetime.utcnow()

        if 'usd_cost' not in params:
            # check original currency
            # exchange rate applied to usd cost

            params['usd_cost'] = params['original_cost']

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
    @append_query_filter(['cost_id', 'cost_key', 'original_currency', 'provider', 'region_code', 'product',
                          'account', 'type', 'resource_group', 'resource', 'service_account_id', 'project_id',
                          'data_source_id', 'domain_id', 'user_projects'])
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
                'product': 'str',
                'account': 'str',
                'type': 'str',
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

        query = params.get('query', {})
        return self.cost_mgr.stat_costs(query)
