import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.manager.plugin_manager import PluginManager
from spaceone.cost_analysis.manager.data_source_rule_manager import (
    DataSourceRuleManager,
)
from spaceone.cost_analysis.service.data_source_rule_service import (
    DataSourceRuleService,
)
from spaceone.cost_analysis.connector.datasource_plugin_connector import (
    DataSourcePluginConnector,
)
from spaceone.cost_analysis.model.data_source_model import DataSource

_LOGGER = logging.getLogger(__name__)


class DataSourcePluginManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dsp_connector: DataSourcePluginConnector = self.locator.get_connector(
            "DataSourcePluginConnector"
        )

    def initialize(self, endpoint):
        _LOGGER.debug(f"[initialize] data source plugin endpoint: {endpoint}")
        self.dsp_connector.initialize(endpoint)

    def init_plugin(self, options, domain_id):
        plugin_info = self.dsp_connector.init(options, domain_id)

        _LOGGER.debug(f"[plugin_info] {plugin_info}")
        plugin_metadata = plugin_info.get("metadata", {})

        return plugin_metadata

    def verify_plugin(self, options, secret_data, schema, domain_id):
        self.dsp_connector.verify(options, secret_data, schema, domain_id)

    def get_tasks(
        self,
        options: dict,
        secret_id: str,
        secret_data: dict,
        start: str,
        last_synchronized_at: str,
        domain_id: str,
        schema: str = None,
        linked_accounts: list = None,
    ) -> tuple:
        response = self.dsp_connector.get_tasks(
            options,
            secret_data,
            schema,
            domain_id,
            start,
            last_synchronized_at,
            linked_accounts,
        )
        tasks = response.get("tasks", [])

        for task in tasks:
            task.update({"secret_id": secret_id, "schema": schema})

        return tasks, response.get("changed", []), response.get("synced_accounts", [])

    def get_linked_accounts(
        self, options: dict, secret_data: dict, domain_id: str, schema: dict = None
    ) -> dict:
        return self.dsp_connector.get_linked_accounts(
            options, secret_data, domain_id, schema
        )

    def get_cost_data(self, options, secret_data, schema, task_options, domain_id):
        return self.dsp_connector.get_cost_data(
            options, secret_data, schema, task_options, domain_id
        )

    def get_data_source_plugin_endpoint_by_vo(self, data_source_vo: DataSource):
        plugin_info = data_source_vo.plugin_info.to_dict()
        endpoint, updated_version = self.get_data_source_plugin_endpoint(
            plugin_info, data_source_vo.domain_id
        )

        if updated_version:
            _LOGGER.debug(
                f'[get_data_source_plugin_endpoint_by_vo] upgrade plugin version: {plugin_info["version"]} -> {updated_version}'
            )
            self.upgrade_data_source_plugin_version(
                data_source_vo, endpoint, updated_version
            )

        return endpoint

    def get_data_source_plugin_endpoint(self, plugin_info, domain_id):
        plugin_mgr: PluginManager = self.locator.get_manager("PluginManager")
        return plugin_mgr.get_plugin_endpoint(plugin_info, domain_id)

    def upgrade_data_source_plugin_version(
        self, data_source_vo: DataSource, endpoint, updated_version
    ):
        plugin_info = data_source_vo.plugin_info.to_dict()

        resource_group = data_source_vo.resource_group
        data_source_id = data_source_vo.data_source_id
        workspace_id = data_source_vo.workspace_id
        domain_id = data_source_vo.domain_id

        self.initialize(endpoint)

        plugin_options = plugin_info.get("options", {})

        plugin_metadata = self.init_plugin(plugin_options, domain_id)
        plugin_info["version"] = updated_version
        plugin_info["metadata"] = plugin_metadata
        data_source_vo.update({"plugin_info": plugin_info})

        self.delete_data_source_rules(data_source_id, domain_id)
        self.create_data_source_rules_by_metadata(
            plugin_metadata, resource_group, data_source_id, workspace_id, domain_id
        )

    def delete_data_source_rules(self, data_source_id, domain_id):
        _LOGGER.debug(
            f"[_delete_data_source_rules] delete all data source rules: {data_source_id}"
        )
        data_source_rule_mgr: DataSourceRuleManager = self.locator.get_manager(
            "DataSourceRuleManager"
        )
        old_data_source_rule_vos = data_source_rule_mgr.filter_data_source_rules(
            data_source_id=data_source_id, rule_type="MANAGED", domain_id=domain_id
        )

        old_data_source_rule_vos.delete()

    def create_data_source_rules_by_metadata(
        self,
        metadata: dict,
        resource_group: str,
        data_source_id: str,
        workspace_id: str,
        domain_id: str,
    ):
        data_source_rules = metadata.get("data_source_rules", [])

        if len(data_source_rules) > 0:
            _LOGGER.debug(
                f"[_create_data_source_rules_by_metadata] create data source rules: {data_source_id} / "
                f"rule count = {len(data_source_rules)}"
            )

            data_source_rule_svc: DataSourceRuleService = self.locator.get_service(
                "DataSourceRuleService", metadata
            )
            for data_source_rule_params in data_source_rules:
                data_source_rule_params["resource_group"] = resource_group
                data_source_rule_params["data_source_id"] = data_source_id
                data_source_rule_params["workspace_id"] = workspace_id
                data_source_rule_params["domain_id"] = domain_id
                data_source_rule_params["rule_type"] = "MANAGED"
                data_source_rule_svc.create_data_source_rule(data_source_rule_params)
