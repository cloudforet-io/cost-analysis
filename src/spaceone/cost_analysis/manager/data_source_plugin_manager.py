import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.plugin_manager import PluginManager
from spaceone.cost_analysis.connector.datasource_plugin_connector import DataSourcePluginConnector
from spaceone.cost_analysis.model.data_source_model import DataSource

_LOGGER = logging.getLogger(__name__)


class DataSourcePluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dsp_connector: DataSourcePluginConnector = self.locator.get_connector('DataSourcePluginConnector')

    def initialize(self, endpoint):
        _LOGGER.debug(f'[initialize] data source plugin endpoint: {endpoint}')
        self.dsp_connector.initialize(endpoint)

    def init_plugin(self, options):
        plugin_info = self.dsp_connector.init(options)

        _LOGGER.debug(f'[plugin_info] {plugin_info}')
        plugin_metadata = plugin_info.get('metadata', {})

        return plugin_metadata

    def verify_plugin(self, options, secret_data, schema):
        self.dsp_connector.verify(options, secret_data, schema)

    def get_data_source_plugin_endpoint_by_vo(self, data_source_vo: DataSource):
        plugin_info = data_source_vo.plugin_info.to_dict()
        endpoint, updated_version = self.get_data_source_plugin_endpoint(plugin_info, data_source_vo.domain_id)

        if updated_version:
            _LOGGER.debug(f'[get_data_source_plugin_endpoint_by_vo] upgrade plugin version: {plugin_info["version"]} -> {updated_version}')
            self.upgrade_data_source_plugin_version(data_source_vo, endpoint, updated_version)

        return endpoint

    def get_data_source_plugin_endpoint(self, plugin_info, domain_id):
        plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')
        return plugin_mgr.get_plugin_endpoint(plugin_info, domain_id)

    def upgrade_data_source_plugin_version(self, data_source_vo: DataSource, endpoint, updated_version):
        plugin_info = data_source_vo.plugin_info.to_dict()
        self.initialize(endpoint)

        plugin_options = plugin_info.get('options', {})

        plugin_metadata = self.init_plugin(plugin_options)
        plugin_info['version'] = updated_version
        plugin_info['metadata'] = plugin_metadata
        data_source_vo.update({'plugin_info': plugin_info})
