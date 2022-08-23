import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.cost_query_set_manager import CostQuerySetManager
from spaceone.cost_analysis.model.cost_query_set_model import CostQuerySet

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostQuerySetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_query_set_mgr: CostQuerySetManager = self.locator.get_manager('CostQuerySetManager')

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['name', 'options', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register cost_query_set

        Args:
            params (dict): {
                'name': 'str',
                'options': 'str',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            cost_query_set_vo (object)
        """

        params['user_id'] = self.transaction.get_meta('user_id')

        return self.cost_query_set_mgr.create_cost_query_set(params)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['cost_query_set_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'name': 'str',
                'options': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            cost_query_set_vo (object)
        """
        cost_query_set_id = params['cost_query_set_id']
        domain_id = params['domain_id']

        cost_query_set_vo: CostQuerySet = self.cost_query_set_mgr.get_cost_query_set(cost_query_set_id, domain_id)

        return self.cost_query_set_mgr.update_cost_query_set_by_vo(params, cost_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['cost_query_set_id', 'domain_id'])
    def delete(self, params):
        """Deregister cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.cost_query_set_mgr.delete_cost_query_set(params['cost_query_set_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['cost_query_set_id', 'domain_id'])
    def get(self, params):
        """ Get cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            cost_query_set_vo (object)
        """

        cost_query_set_id = params['cost_query_set_id']
        domain_id = params['domain_id']

        return self.cost_query_set_mgr.get_cost_query_set(cost_query_set_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['domain_id'])
    @append_query_filter(['cost_query_set_id', 'name', 'user_id', 'domain_id'])
    @append_keyword_filter(['cost_query_set_id', 'name'])
    def list(self, params):
        """ List cost_query_sets

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'name': 'str',
                'user_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            cost_query_set_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.cost_query_set_mgr.list_cost_query_sets(query)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['cost_query_set_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.cost_query_set_mgr.stat_cost_query_sets(query)
