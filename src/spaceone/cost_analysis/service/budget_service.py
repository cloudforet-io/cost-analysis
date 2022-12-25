import logging
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY, YEARLY

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
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
    # @change_date_value(['start', 'end'])
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
                'start': 'str',
                'end': 'str',
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
        notifications = params.get('notifications', [])

        self._check_target(project_id, project_group_id, domain_id)
        self._check_time_period(start, end)

        if time_unit == 'TOTAL':
            if limit is None:
                raise ERROR_REQUIRED_PARAMETER(key='limit')

            params['planned_limits'] = None

        else:
            # Check Planned Limits
            self._check_planned_limits(start, end, time_unit, planned_limits)

            params['limit'] = 0
            for planned_limit in planned_limits:
                params['limit'] += planned_limit.get('limit', 0)

        # Check Notifications
        self._check_notifications(notifications)

        budget_vo = self.budget_mgr.create_budget(params)

        # Create budget usages
        budget_usage_mgr: BudgetUsageManager = self.locator.get_manager('BudgetUsageManager')
        budget_usage_mgr.create_budget_usages(budget_vo)
        budget_usage_mgr.update_cost_usage(budget_vo)

        return budget_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['budget_id', 'domain_id'])
    # @change_date_value(['end'])
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
        planned_limits = params.get('planned_limits')

        budget_usage_mgr: BudgetUsageManager = self.locator.get_manager('BudgetUsageManager')

        budget_vo: Budget = self.budget_mgr.get_budget(budget_id, domain_id)

        if end:
            if budget_vo.end > end:
                raise

            if planned_limits is None:
                raise

        # Check limit and Planned Limits

        budget_vo = self.budget_mgr.update_budget_by_vo(params, budget_vo)

        if 'name' in params:
            budget_usage_vos = budget_usage_mgr.filter_budget_usages(budget_id=budget_id)
            for budget_usage_vo in budget_usage_vos:
                budget_usage_mgr.update_budget_usage_by_vo({'name': params['name']}, budget_usage_vo)

        return budget_vo

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
        notifications = params.get('notifications', [])

        # Check Notifications
        self._check_notifications(notifications)
        params['notifications'] = notifications

        budget_vo: Budget = self.budget_mgr.get_budget(budget_id, domain_id)

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

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['budget_id', 'name', 'project_id', 'project_group_id', 'time_unit', 'domain_id'])
    @append_keyword_filter(['budget_id', 'name'])
    def list(self, params):
        """ List budgets

        Args:
            params (dict): {
                'budget_id': 'str',
                'name': 'str',
                'project_id': 'str',
                'project_group_id': 'str',
                'time_unit': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta,
                'user_project_groups': 'list', // from meta
            }

        Returns:
            budget_vos (object)
            total_count
        """

        query = self._set_user_project_or_project_group_filter(params)
        return self.budget_mgr.list_budgets(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        query = self._set_user_project_or_project_group_filter(params)
        return self.budget_mgr.stat_budgets(query)

    def _check_target(self, project_id, project_group_id, domain_id):
        if project_id is None and project_group_id is None:
            raise ERROR_REQUIRED_PARAMETER(key='project_id or project_group_id')

        if project_id and project_group_id:
            raise ERROR_ONLY_ONF_OF_PROJECT_OR_PROJECT_GROUP()

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        if project_id:
            identity_mgr.get_project(project_id, domain_id)
        else:
            identity_mgr.get_project_group(project_group_id, domain_id)

    @staticmethod
    def _check_time_period(start, end):
        if start >= end:
            raise ERROR_INVALID_TIME_RANGE(start=start, end=end)

    def _check_planned_limits(self, start, end, time_unit, planned_limits):
        planned_limits_dict = self._convert_planned_limits_data_type(planned_limits)

        if time_unit == 'MONTHLY':
            date_format = '%Y-%m'
        else:
            date_format = '%Y'

        try:
            start_dt = datetime.strptime(start, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key='start', type=date_format)

        try:
            end_dt = datetime.strptime(end, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key='end', type=date_format)

        for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt):
            date_str = dt.strftime(date_format)
            if date_str not in planned_limits_dict:
                raise ERROR_NO_DATE_IN_PLANNED_LIMITS(date=date_str)

            del planned_limits_dict[date_str]

        if len(planned_limits_dict.keys()) > 0:
            raise ERROR_DATE_IS_WRONG(date=list(planned_limits_dict.keys()))

    @staticmethod
    def _convert_planned_limits_data_type(planned_limits):
        planned_limits_dict = {}

        for planned_limit in planned_limits:
            date = planned_limit.get('date')
            limit = planned_limit.get('limit', 0)
            if date is None:
                raise ERROR_DATE_IS_REQUIRED(value=planned_limit)

            if limit < 0:
                raise ERROR_LIMIT_IS_WRONG(value=planned_limit)

            planned_limits_dict[date] = limit

        return planned_limits_dict

    @staticmethod
    def _check_notifications(notifications):
        for notification in notifications:
            unit = notification.get('unit')
            notification_type = notification.get('notification_type')
            threshold = notification.get('threshold', 0)

            if unit not in ['PERCENT', 'ACTUAL_COST']:
                raise ERROR_UNIT_IS_REQUIRED(value=notification)

            if notification_type not in ['CRITICAL', 'WARNING']:
                raise ERROR_NOTIFICATION_TYPE_IS_REQUIRED(value=notification)

            if threshold < 0:
                raise ERROR_THRESHOLD_IS_WRONG(value=notification)

            if unit == 'PERCENT':
                if threshold > 100:
                    raise ERROR_THRESHOLD_IS_WRONG_IN_PERCENT_TYPE(value=notification)

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
