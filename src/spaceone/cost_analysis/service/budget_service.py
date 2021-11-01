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
    @check_required(['time_unit', 'start', 'end', 'domain_id'])
    @change_date_value(['start', 'end'])
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
                'start': 'date',
                'end': 'date',
                'notifications': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """

        domain_id = params['domain_id']
        project_id = params.get('project_id')
        project_group_id = params.get('project_group_id')
        limit = params.get('limit')
        planned_limits = params.get('planned_limits', [])
        time_unit = params['time_unit']
        start = params['start']
        end = params['end']

        self._check_target(project_id, project_group_id, domain_id)
        self._check_time_period(start, end)

        if time_unit == 'TOTAL':
            if limit is None:
                raise ERROR_REQUIRED_PARAMETER(key='limit')

            params['planned_limits'] = None

        else:
            # Check Planned Limits

            params['limit'] = 0
            for planned_limit in planned_limits:
                params['limit'] += planned_limit['limit']

        # Check Notifications

        return self.budget_mgr.create_budget(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update budget

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'limit': 'float',
                'planned_limits': 'list',
                'end': 'date',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """
        budget_id = params['budget_id']
        domain_id = params['domain_id']
        end = params.get('end')

        budget_vo: Budget = self.budget_mgr.get_budget(budget_id, domain_id)

        # Check limit and Planned Limits

        if end:
            # reset total_usd_cost and monthly_costs
            pass

        return self.budget_mgr.update_budget_by_vo(params, budget_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    def set_notification(self, params):
        """Set budget notification

        Args:
            params (dict): {
                'budget_id': 'str',
                'notifications': 'list',
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """
        budget_id = params['budget_id']
        domain_id = params['domain_id']
        budget_vo: Budget = self.budget_mgr.get_budget(budget_id, domain_id)

        params['notifications'] = params.get('notifications', [])

        # Check Notifications

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

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {
            'user_projects': 'authorization.projects',
            'user_project_groups': 'authorization.project_groups'
        }
    })
    @check_required(['domain_id'])
    @append_query_filter(['budget_id', 'name', 'project_id', 'project_group_id', 'domain_id',
                          'user_projects', 'user_project_groups'])
    @append_keyword_filter(['budget_id', 'name'])
    def list(self, params):
        """ List budgets

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'project_id': 'str',
                'project_group_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta,
                'user_project_groups': 'list', // from meta
            }

        Returns:
            budget_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.budget_mgr.list_budgets(query)

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {
            'user_projects': 'authorization.projects',
            'user_project_groups': 'authorization.project_groups'
        }
    })
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['budget_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta,
                'user_project_groups': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.budget_mgr.stat_budgets(query)

    def _check_target(self, project_id, project_group_id, domain_id):
        if project_id and project_group_id:
            raise ERROR_ONLY_ONF_OF_PROJECT_OR_PROJECT_GROUP()

        # Check Project ID and Project Group ID

    def _check_time_period(self, start, end):
        pass
        # Check Time Period
