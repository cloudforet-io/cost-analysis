import logging

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.repository_manager import RepositoryManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager
from spaceone.cost_analysis.manager.data_source_plugin_manager import DataSourcePluginManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.model.data_source_model import DataSource

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager('DataSourcePluginManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'data_source_type', 'domain_id'])
    def register(self, params):
        """Register data source

        Args:
            params (dict): {
                'name': 'str',
                'data_source_type': 'str',
                'provider': 'str',
                'template': 'dict',
                'plugin_info': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        domain_id = params['domain_id']
        data_source_type = params['data_source_type']

        if data_source_type == 'EXTERNAL':
            plugin_info = params.get('plugin_info', {})

            self._validate_plugin_info(plugin_info)
            self._check_plugin(plugin_info['plugin_id'], domain_id)

            # Update metadata
            endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)
            if updated_version:
                params['plugin_info']['version'] = updated_version

            options = params['plugin_info'].get('options', {})
            plugin_metadata = self._init_plugin(endpoint, options)

            params['plugin_info']['metadata'] = plugin_metadata

            # set template from plugin metadata
            # set data source rule from plugin metadata

        else:
            params['plugin_info'] = None

        if 'template' in params:
            # check template
            pass

        return self.data_source_mgr.register_data_source(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def update(self, params):
        """Update data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'template': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """
        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if 'template' in params:
            # check template
            pass

        return self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def enable(self, params):
        """ Enable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'ENABLED'}, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def disable(self, params):
        """ Disable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'DISABLED'}, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def deregister(self, params):
        """Deregister data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.data_source_mgr.deregister_data_source(params['data_source_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def verify_plugin(self, params):
        """ Verify data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.data_source_type == 'LOCAL':
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(data_source_vo)
        plugin_info = data_source_vo.plugin_info.to_dict()

        self._verify_plugin(endpoint, plugin_info, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def update_plugin(self, params):
        """Update data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',
                'version': 'str',
                'options': 'dict',
                'upgrade_mode': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        version = params.get('version')
        options = params.get('options')
        upgrade_mode = params.get('upgrade_mode')

        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.data_source_type == 'LOCAL':
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        plugin_info = data_source_vo.plugin_info.to_dict()

        if version:
            plugin_info['version'] = version

        if options:
            plugin_info['options'] = options

        if upgrade_mode:
            plugin_info['upgrade_mode'] = upgrade_mode

        endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)
        if updated_version:
            plugin_info['version'] = updated_version

        options = plugin_info.get('options', {})
        plugin_metadata = self._init_plugin(endpoint, options)
        plugin_info['metadata'] = plugin_metadata

        params = {
            'plugin_info': plugin_info
        }

        # set template from plugin metadata
        # set data source rule from plugin metadata

        _LOGGER.debug(f'[update_plugin] {plugin_info}')

        return self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def get(self, params):
        """ Get data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']

        return self.data_source_mgr.get_data_source(data_source_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['data_source_id', 'name', 'state', 'data_source_type', 'provider', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name'])
    def list(self, params):
        """ List data sources

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'state': 'str',
                'cost_analysis_type': 'str',
                'provider': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            data_source_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.data_source_mgr.list_data_sources(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name'])
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
        return self.data_source_mgr.stat_data_sources(query)

    @staticmethod
    def _validate_plugin_info(plugin_info):
        if 'plugin_id' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.plugin_id')

        if plugin_info.get('upgrade_mode', 'AUTO') == 'MANUAL' and 'version' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.version')

    def _check_plugin(self, plugin_id, domain_id):
        repo_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')
        repo_mgr.get_plugin(plugin_id, domain_id)

    def _init_plugin(self, endpoint, options):
        self.ds_plugin_mgr.initialize(endpoint)
        return self.ds_plugin_mgr.init_plugin(options)

    def _verify_plugin(self, endpoint, plugin_info, domain_id):
        options = plugin_info.get('options', {})
        secret_id = plugin_info.get('secret_id')

        if secret_id:
            secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
            secret_info = secret_mgr.get_secret(secret_id, domain_id)
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
            schema = secret_info.get('schema')
        else:
            secret_data = {}
            schema = None

        ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager('DataSourcePluginManager')
        ds_plugin_mgr.initialize(endpoint)
        ds_plugin_mgr.verify_plugin(options, secret_data, schema)
