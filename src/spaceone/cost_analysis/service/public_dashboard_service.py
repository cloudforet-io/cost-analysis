import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.public_dashboard_manager import PublicDashboardManager
from spaceone.cost_analysis.model.public_dashboard_model import PublicDashboard

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.public_dashboard_mgr: PublicDashboardManager = self.locator.get_manager('PublicDashboardManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'period_type', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register public_dashboard

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
            public_dashboard_vo (object)
        """

        default_layout_id = params.get('default_layout_id')

        if default_layout_id:
            params['custom_layouts'] = []

        if params['period_type'] == 'FIXED':
            if 'period' not in params:
                raise ERROR_REQUIRED_PARAMETER(key='period')

        else:
            params['period'] = None

        return self.public_dashboard_mgr.create_public_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['public_dashboard_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update public_dashboard

        Args:
            params (dict): {
                'public_dashboard_id': 'str',
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
            public_dashboard_vo (object)
        """
        public_dashboard_id = params['public_dashboard_id']
        domain_id = params['domain_id']
        default_layout_id = params.get('default_layout_id')
        custom_layouts = params.get('custom_layouts')
        period_type = params.get('period_type')

        public_dashboard_vo: PublicDashboard = self.public_dashboard_mgr.get_public_dashboard(public_dashboard_id, domain_id)

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

        return self.public_dashboard_mgr.update_public_dashboard_by_vo(params, public_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['public_dashboard_id', 'domain_id'])
    def delete(self, params):
        """Deregister public_dashboard

        Args:
            params (dict): {
                'public_dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.public_dashboard_mgr.delete_public_dashboard(params['public_dashboard_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['public_dashboard_id', 'domain_id'])
    def get(self, params):
        """ Get public_dashboard

        Args:
            params (dict): {
                'public_dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            public_dashboard_vo (object)
        """

        public_dashboard_id = params['public_dashboard_id']
        domain_id = params['domain_id']

        return self.public_dashboard_mgr.get_public_dashboard(public_dashboard_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['public_dashboard_id', 'name', 'domain_id'])
    @append_keyword_filter(['public_dashboard_id', 'name'])
    def list(self, params):
        """ List public_dashboards

        Args:
            params (dict): {
                'public_dashboard_id': 'str',
                'name': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            public_dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.public_dashboard_mgr.list_public_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['public_dashboard_id', 'name'])
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
        return self.public_dashboard_mgr.stat_public_dashboards(query)
