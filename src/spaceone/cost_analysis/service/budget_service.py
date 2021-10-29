import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.model.budget_model import Budget

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BudgetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_mgr: BudgetManager = self.locator.get_manager('BudgetManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['time_unit', 'domain_id'])
    def create(self, params):
        """Register budget

        Args:
            params (dict): {
                'name': 'str',
                'project_id': 'str',
                'project_group_id': 'str',
                'limit': 'float',
                'planned_limits': 'list',
                'cost_types': 'dict',
                'time_unit': 'str',
                'time_period': 'dict',
                'notifications': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """

        return self.budget_mgr.create_budget(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    def update(self, params):
        """Update budget

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'limit': 'float',
                'planned_limits': 'list',
                'time_period': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """
        budget_id = params['budget_id']
        domain_id = params['domain_id']
        budget_vo: Budget = self.budget_mgr.get_budget(budget_id, domain_id)

        if 'time_period' in params:
            # check time_period
            # reset total_usd_cost and monthly_costs
            pass

        return self.budget_mgr.update_budget_by_vo(params, budget_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    def delete(self, params):
        """Deregister budget

        Args:
            params (dict): {
                'budget_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.budget_mgr.delete_budget(params['budget_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    def get(self, params):
        """ Get budget

        Args:
            params (dict): {
                'budget_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            budget_vo (object)
        """

        budget_id = params['budget_id']
        domain_id = params['domain_id']

        return self.budget_mgr.get_budget(budget_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['budget_id', 'name', 'state', 'budget_type', 'provider', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['budget_id', 'name'])
    def list(self, params):
        """ List budgets

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'state': 'str',
                'cost_analysis_type': 'str',
                'provider': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            budget_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.budget_mgr.list_budgets(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['budget_id', 'name'])
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
        return self.budget_mgr.stat_budgets(query)
