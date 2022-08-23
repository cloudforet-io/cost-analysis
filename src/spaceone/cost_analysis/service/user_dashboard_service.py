import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.user_dashboard_manager import UserDashboardManager
from spaceone.cost_analysis.model.user_dashboard_model import UserDashboard

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class UserDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_dashboard_mgr: UserDashboardManager = self.locator.get_manager('UserDashboardManager')

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['name', 'period_type', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register user_dashboard

        Args:
            params (dict): {
                'name': 'str',
                'default_layout_id': 'str',
                'custom_layouts': 'list',
                'default_filter': 'dict',
                'period_type': 'str',
                'period': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            user_dashboard_vo (object)
        """

        params['user_id'] = self.transaction.get_meta('user_id')

        default_layout_id = params.get('default_layout_id')

        if default_layout_id:
            params['custom_layouts'] = []

        if params['period_type'] == 'FIXED':
            if 'period' not in params:
                raise ERROR_REQUIRED_PARAMETER(key='period')

        else:
            params['period'] = None

        return self.user_dashboard_mgr.create_user_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['user_dashboard_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update user_dashboard

        Args:
            params (dict): {
                'user_dashboard_id': 'str',
                'name': 'str',
                'default_layout_id': 'str',
                'custom_layouts': 'list',
                'default_filter': 'dict',
                'period_type': 'str',
                'period': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            user_dashboard_vo (object)
        """
        user_dashboard_id = params['user_dashboard_id']
        domain_id = params['domain_id']
        default_layout_id = params.get('default_layout_id')
        custom_layouts = params.get('custom_layouts')
        period_type = params.get('period_type')

        user_dashboard_vo: UserDashboard = self.user_dashboard_mgr.get_user_dashboard(user_dashboard_id, domain_id)

        if default_layout_id:
            params['custom_layout_id'] = []
        else:
            if custom_layouts:
                params['default_layout_id'] = None

        if period_type:
            if period_type == 'FIXED':
                if 'period' not in params:
                    raise ERROR_REQUIRED_PARAMETER(key='period')

            else:
                params['period'] = None

        return self.user_dashboard_mgr.update_user_dashboard_by_vo(params, user_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['user_dashboard_id', 'domain_id'])
    def delete(self, params):
        """Deregister user_dashboard

        Args:
            params (dict): {
                'user_dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.user_dashboard_mgr.delete_user_dashboard(params['user_dashboard_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['user_dashboard_id', 'domain_id'])
    def get(self, params):
        """ Get user_dashboard

        Args:
            params (dict): {
                'user_dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            user_dashboard_vo (object)
        """

        user_dashboard_id = params['user_dashboard_id']
        domain_id = params['domain_id']

        return self.user_dashboard_mgr.get_user_dashboard(user_dashboard_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['domain_id'])
    @append_query_filter(['user_dashboard_id', 'name', 'user_id', 'domain_id'])
    @append_keyword_filter(['user_dashboard_id', 'name'])
    def list(self, params):
        """ List user_dashboards

        Args:
            params (dict): {
                'user_dashboard_id': 'str',
                'name': 'str',
                'scope': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            user_dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.user_dashboard_mgr.list_user_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['user_dashboard_id', 'name'])
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
        return self.user_dashboard_mgr.stat_user_dashboards(query)
