import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.custom_widget_manager import CustomWidgetManager
from spaceone.cost_analysis.model.custom_widget_model import CustomWidget

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CustomWidgetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_widget_mgr: CustomWidgetManager = self.locator.get_manager('CustomWidgetManager')

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['name', 'options', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register custom_widget

        Args:
            params (dict): {
                'name': 'str',
                'options': 'str',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """

        params['user_id'] = self.transaction.get_meta('user_id')

        return self.custom_widget_mgr.create_custom_widget(params)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update custom_widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'name': 'str',
                'options': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """
        custom_widget_id = params['custom_widget_id']
        domain_id = params['domain_id']

        custom_widget_vo: CustomWidget = self.custom_widget_mgr.get_custom_widget(custom_widget_id, domain_id)

        return self.custom_widget_mgr.update_custom_widget_by_vo(params, custom_widget_vo)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    def delete(self, params):
        """Deregister custom_widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.custom_widget_mgr.delete_custom_widget(params['custom_widget_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    def get(self, params):
        """ Get custom_widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            custom_widget_vo (object)
        """

        custom_widget_id = params['custom_widget_id']
        domain_id = params['domain_id']

        return self.custom_widget_mgr.get_custom_widget(custom_widget_id, domain_id, params.get('only'))

    @transaction(append_meta={
        'authorization.scope': 'USER',
        'mutation.append_parameter': {'user_self': 'user_id'}
    })
    @check_required(['domain_id'])
    @append_query_filter(['custom_widget_id', 'name', 'user_id', 'domain_id', 'user_self'])
    @append_keyword_filter(['custom_widget_id', 'name'])
    def list(self, params):
        """ List custom_widgets

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'name': 'str',
                'user_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_self': 'list', // from meta
            }

        Returns:
            custom_widget_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.custom_widget_mgr.list_custom_widgets(query)

    @transaction(append_meta={
        'authorization.scope': 'USER',
        'mutation.append_parameter': {'user_self': 'user_id'}
    })
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_self'])
    @append_keyword_filter(['custom_widget_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_self': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.custom_widget_mgr.stat_custom_widgets(query)
