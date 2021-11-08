import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.dashboard_manager import DashboardManager
from spaceone.cost_analysis.model.dashboard_model import Dashboard

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_mgr: DashboardManager = self.locator.get_manager('DashboardManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register dashboard

        Args:
            params (dict): {
                'name': 'str',
                'default_layout_id': 'str',
                'custom_layouts': 'list',
                'default_filter': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            dashboard_vo (object)
        """
        params['user_id'] = self.transaction.get_meta('user_id')

        default_layout_id = params.get('default_layout_id')

        if default_layout_id:
            params['custom_layouts'] = []

        return self.dashboard_mgr.create_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['dashboard_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'name': 'str',
                'default_layout_id': 'str',
                'custom_layouts': 'list',
                'default_filter': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            dashboard_vo (object)
        """
        dashboard_id = params['dashboard_id']
        domain_id = params['domain_id']
        default_layout_id = params.get('default_layout_id')
        custom_layouts = params.get('custom_layouts')

        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(dashboard_id, domain_id)

        if default_layout_id:
            params['custom_layout_id'] = []
        else:
            if custom_layouts:
                params['default_layout_id'] = None

        return self.dashboard_mgr.update_dashboard_by_vo(params, dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['dashboard_id', 'scope', 'domain_id'])
    def change_scope(self, params):
        """Change dashboard scope

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'scope': 'str',
                'domain_id': 'str'
            }

        Returns:
            dashboard_vo (object)
        """
        dashboard_id = params['dashboard_id']
        domain_id = params['domain_id']
        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(dashboard_id, domain_id)

        return self.dashboard_mgr.update_dashboard_by_vo(params, dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['dashboard_id', 'domain_id'])
    def delete(self, params):
        """Deregister dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.dashboard_mgr.delete_dashboard(params['dashboard_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['dashboard_id', 'domain_id'])
    def get(self, params):
        """ Get dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            dashboard_vo (object)
        """

        dashboard_id = params['dashboard_id']
        domain_id = params['domain_id']

        return self.dashboard_mgr.get_dashboard(dashboard_id, domain_id, params.get('only'))

    @transaction(append_meta={
        'authorization.scope': 'DOMAIN'
    })
    @check_required(['domain_id'])
    @append_query_filter(['dashboard_id', 'name', 'scope', 'user_id', 'domain_id'])
    @append_keyword_filter(['dashboard_id', 'name'])
    def list(self, params):
        """ List dashboards

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'name': 'str',
                'scope': 'str',
                'user_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.dashboard_mgr.list_dashboards(query)

    @transaction(append_meta={
        'authorization.scope': 'DOMAIN'
    })
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['dashboard_id', 'name'])
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
        return self.dashboard_mgr.stat_dashboards(query)
