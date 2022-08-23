import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BudgetUsageService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_usage_mgr: BudgetUsageManager = self.locator.get_manager('BudgetUsageManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['budget_id', 'name', 'date', 'domain_id'])
    @append_keyword_filter(['budget_id', 'name'])
    def list(self, params):
        """ List budget_usages

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'date': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta,
                'user_project_groups': 'list', // from meta
            }

        Returns:
            budget_usage_vos (object)
            total_count
        """

        query = self._set_user_project_or_project_group_filter(params)
        return self.budget_usage_mgr.list_budget_usages(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['budget_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'budget_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta,
                'user_project_groups': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = self._set_user_project_or_project_group_filter(params)
        return self.budget_usage_mgr.stat_budget_usages(query)

    @staticmethod
    def _set_user_project_or_project_group_filter(params):
        query = params.get('query', {})
        query['filter'] = query.get('filter', [])

        if 'user_projects' in params:
            user_projects = params['user_projects'] + [None]
            query['filter'].append({'k': 'user_projects', 'v': user_projects, 'o': 'in'})

        if 'user_project_groups' in params:
            user_project_groups = params['user_project_groups'] + [None]
            query['filter'].append({'k': 'user_project_groups', 'v': user_project_groups, 'o': 'in'})

        return query
