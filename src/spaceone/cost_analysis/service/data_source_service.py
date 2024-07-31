import copy
import logging
from typing import Tuple, Union

from mongoengine import QuerySet
from spaceone.core.service import *
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.data_source.request import *
from spaceone.cost_analysis.model.data_source.response import *
from spaceone.cost_analysis.service.job_service import JobService
from spaceone.cost_analysis.manager.repository_manager import RepositoryManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager
from spaceone.cost_analysis.manager.data_source_plugin_manager import (
    DataSourcePluginManager,
)
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.model.data_source_model import DataSource
from spaceone.cost_analysis.manager.data_source_account_manager import (
    DataSourceAccountManager,
)
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.job_manager import JobManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceService(BaseService):
    resource = "DataSource"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr = DataSourceManager()
        self.data_source_account_mgr = DataSourceAccountManager()
        self.ds_plugin_mgr = DataSourcePluginManager()
        self.cost_mgr = CostManager()
        self.budget_usage_mgr = BudgetUsageManager()
        self.job_mgr = JobManager()

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def register(
        self, params: DataSourceRegisterRequest
    ) -> Union[DataSourceResponse, dict]:
        """Register data source

        Args:
            params (dict): {
                'name': 'str',              # required
                'data_source_type': 'str',  # required
                'provider': 'str',
                'secret_type': 'str',
                'secret_filter': 'dict',
                'template': 'dict',
                'plugin_info': 'dict',
                'tags': 'dict',
                'resource_group': 'str      # required
                'workspace_id': 'str'
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            data_source_vo (object)
        """
        params = params.dict(exclude_unset=True)

        domain_id = params["domain_id"]
        data_source_type = params["data_source_type"]
        resource_group = params["resource_group"]

        secret_data = {}

        # Check permission by resource group
        if resource_group == "WORKSPACE":
            identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
            identity_mgr.check_workspace(params["workspace_id"], domain_id)
        else:
            params["workspace_id"] = "*"

        if data_source_type == "EXTERNAL":
            params["template"] = None

            plugin_info = params.get("plugin_info", {})
            secret_type = params.get("secret_type", "MANUAL")

            if secret_type == "USE_SERVICE_ACCOUNT_SECRET" and "provider" not in params:
                raise ERROR_REQUIRED_PARAMETER(key="provider")

            self._validate_plugin_info(plugin_info, secret_type)
            self._check_plugin(plugin_info["plugin_id"])

            if "secret_filter" in params:
                self.validate_secret_filter(
                    params["secret_filter"], params["domain_id"]
                )

            # Update metadata
            (
                endpoint,
                updated_version,
            ) = self.ds_plugin_mgr.get_data_source_plugin_endpoint(
                plugin_info, domain_id
            )
            if updated_version:
                params["plugin_info"]["version"] = updated_version

            options = params["plugin_info"].get("options", {})

            plugin_metadata = self._init_plugin(endpoint, options, domain_id)

            params["plugin_info"]["metadata"] = plugin_metadata

            secret_data = plugin_info.get("secret_data")
            if secret_type == "MANUAL" and secret_data:
                self._verify_plugin(endpoint, plugin_info, domain_id)

                secret_mgr: SecretManager = self.locator.get_manager("SecretManager")

                create_secret_params = {
                    "data": secret_data,
                    "resource_group": resource_group,
                    "schema_id": plugin_info.get("schema_id"),
                    "workspace_id": params["workspace_id"],
                }
                secret_info = secret_mgr.create_secret(create_secret_params, domain_id)

                params["plugin_info"]["secret_id"] = secret_info["secret_id"]
                del params["plugin_info"]["secret_data"]

        else:
            params["plugin_info"] = None
            params["secret_type"] = None
            params["secret_filter"] = None

            if template := params.get("template"):
                # Check Template
                pass
            else:
                raise ERROR_REQUIRED_PARAMETER(key="template")

        data_source_vo: DataSource = self.data_source_mgr.register_data_source(params)

        # Create DataSourceRules
        if data_source_type == "EXTERNAL":
            resource_group = data_source_vo.resource_group
            workspace_id = data_source_vo.workspace_id
            data_source_id = data_source_vo.data_source_id
            metadata = data_source_vo.plugin_info.metadata

            self.ds_plugin_mgr.create_data_source_rules_by_metadata(
                metadata, resource_group, data_source_id, workspace_id, domain_id
            )

        # Create DataSourceAccount
        if data_source_type == "EXTERNAL":
            plugin_info = params["plugin_info"]
            options = plugin_info.get("options", {})
            metadata = plugin_info.get("metadata", {})

            if metadata.get("use_account_routing", False):
                accounts_info = self.ds_plugin_mgr.get_linked_accounts(
                    options, secret_data, domain_id
                )

                self.create_data_source_account_with_data_source_vo(
                    accounts_info, data_source_vo
                )

                data_source_vo = self.data_source_mgr.update_data_source_account_and_connected_workspace_count_by_vo(
                    data_source_vo
                )

        return DataSourceResponse(**data_source_vo.to_dict())

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def update(self, params):
        """Update data source

        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'name': 'str',
                'secret_filter': 'dict',
                'template': 'dict',
                'tags': 'dict'
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            data_source_vo (object)
        """
        data_source_id = params["data_source_id"]
        domain_id = params["domain_id"]
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id
        )

        if "secret_filter" in params:
            if data_source_vo.secret_type == "USE_SERVICE_ACCOUNT_SECRET":
                self.validate_secret_filter(
                    params["secret_filter"], params["domain_id"]
                )
            else:
                raise ERROR_NOT_ALLOW_SECRET_FILTER(data_source_id=data_source_id)

        if "template" in params:
            if data_source_vo.data_source_type == "LOCAL":
                # Check Template
                pass
            else:
                raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        return self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

    @transaction(
        permission="cost-analysis:DataSource.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def update_permissions(
        self, params: DataSourceUpdatePermissionsRequest
    ) -> Union[DataSourceResponse, dict]:
        """Update data source permissions

        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'permissions': 'dict',      # required
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params.data_source_id
        domain_id = params.domain_id

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id
        )

        deny = params.permissions.get("deny", [])

        if deny:
            params.permissions["deny"] = list(set(deny))

        data_source_vo = self.data_source_mgr.update_data_source_by_vo(
            params.dict(exclude_unset=True), data_source_vo
        )
        return DataSourceResponse(**data_source_vo.to_dict())

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "secret_schema_id", "secret_data", "domain_id"])
    def update_secret_data(self, params: dict) -> DataSource:
        """Update secret data of data source
        Args:
            params (dict): {
                'data_source_id': 'str',        # required
                'secret_schema_id': 'str',      # required
                'secret_data': 'dict',          # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth
            }
        Returns:
            data_source_vo (object)
        """

        secret_data = params["secret_data"]
        secret_schema_id = params["secret_schema_id"]
        data_source_id = params["data_source_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id=data_source_id,
            domain_id=domain_id,
            workspace_id=workspace_id,
        )

        if data_source_vo.secret_type == "MANUAL" and secret_data:
            secret_mgr: SecretManager = self.locator.get_manager("SecretManager")
            # TODO : validate schema

            # Delete old secret
            if secret_id := data_source_vo.plugin_info.secret_id:
                secret_mgr.delete_secret(secret_id, domain_id)

            # Create new secret
            create_secret_params = {
                "schema_id": secret_schema_id,
                "data": secret_data,
                "resource_group": data_source_vo.resource_group,
                "workspace_id": data_source_vo.workspace_id,
            }

            secret_info = secret_mgr.create_secret(create_secret_params, domain_id)
            plugin_info = data_source_vo.plugin_info.to_dict()
            plugin_info.update(
                {"secret_id": secret_info["secret_id"], "schema_id": secret_schema_id}
            )

            self.data_source_mgr.update_data_source_by_vo(
                {"plugin_info": plugin_info}, data_source_vo
            )

        return data_source_vo

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def verify_plugin(self, params):
        """Verify data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params["data_source_id"]
        domain_id = params["domain_id"]
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id
        )

        if data_source_vo.data_source_type == "LOCAL":
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(
            data_source_vo
        )
        plugin_info = data_source_vo.plugin_info.to_dict()

        self._verify_plugin(endpoint, plugin_info, domain_id)

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def update_plugin(self, params):
        """Update data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',  # required
                'version': 'str',
                'options': 'dict',
                'upgrade_mode': 'str',
                'domain_id': 'str'       # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params["data_source_id"]
        domain_id = params["domain_id"]
        version = params.get("version")
        options = params.get("options")
        upgrade_mode = params.get("upgrade_mode")

        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.data_source_type == "LOCAL":
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        plugin_info = data_source_vo.plugin_info.to_dict()

        if version:
            plugin_info["version"] = version

        if isinstance(options, dict):
            plugin_info["options"] = options

        if upgrade_mode:
            plugin_info["upgrade_mode"] = upgrade_mode

        endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(
            plugin_info, domain_id
        )
        if updated_version:
            plugin_info["version"] = updated_version

        options = plugin_info.get("options", {})
        plugin_metadata = self._init_plugin(endpoint, options, domain_id)
        plugin_info["metadata"] = plugin_metadata

        params = {"plugin_info": plugin_info}

        data_source_vo = self.data_source_mgr.update_data_source_by_vo(
            params, data_source_vo
        )

        resource_group = data_source_vo.resource_group
        workspace_id = data_source_vo.workspace_id
        self.ds_plugin_mgr.delete_data_source_rules(data_source_id, domain_id)
        self.ds_plugin_mgr.create_data_source_rules_by_metadata(
            plugin_metadata, resource_group, data_source_id, workspace_id, domain_id
        )

        return data_source_vo

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def enable(self, params):
        """Enable data source

        Args:
            params (dict): {
                'data_source_id': 'str',  # required
                'domain_id': 'str'        # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params["data_source_id"]
        domain_id = params["domain_id"]
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id
        )

        return self.data_source_mgr.update_data_source_by_vo(
            {"state": "ENABLED"}, data_source_vo
        )

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def disable(self, params):
        """Disable data source

        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params["data_source_id"]
        domain_id = params["domain_id"]
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id
        )

        return self.data_source_mgr.update_data_source_by_vo(
            {"state": "DISABLED"}, data_source_vo
        )

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def deregister(self, params):
        """Deregister data source

        Args:
            params (dict): {
                'data_source_id': 'str',        # required
                'cascade_delete_cost: 'bool',
                'workspace_id: 'str',           # injected from auth (optional)
                'domain_id': 'str'              # injected from auth
            }

        Returns:
            None
        """

        data_source_id = params["data_source_id"]
        cascade_delete_cost = params.get("cascade_delete_cost", True)
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id, workspace_id
        )

        if cascade_delete_cost:
            self.cost_mgr.delete_cost_with_datasource(domain_id, data_source_id)
            self.budget_usage_mgr.update_budget_usage(domain_id, data_source_id)
            self.cost_mgr.remove_stat_cache(domain_id, data_source_id)
            self.job_mgr.preload_cost_stat_queries(domain_id, data_source_id)

        if data_source_vo.plugin_info:
            secret_id = data_source_vo.plugin_info.secret_id

            if secret_id:
                secret_mgr: SecretManager = self.locator.get_manager("SecretManager")
                secret_mgr.delete_secret(secret_id, domain_id)

        self.data_source_account_mgr.delete_ds_account_with_data_source(
            data_source_id, domain_id
        )

        self.data_source_mgr.deregister_data_source_by_vo(data_source_vo)

    @transaction(
        permission="cost-analysis:DataSource.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["data_source_id", "domain_id"])
    def sync(self, params):
        """Sync data with data source

        Args:
            params (dict): {
                'data_source_id': 'str',        # required
                'start': 'datetime',
                'no_preload_cache': 'bool',
                'workspace_id: 'str',           # injected from auth (optional)
                'domain_id': 'str'              # injected from auth
            }

        Returns:
            None
        """
        job_service: JobService = self.locator.get_service("JobService")

        data_source_id = params["data_source_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]
        job_options = {
            "no_preload_cache": params.get("no_preload_cache", False),
            "start": params.get("start"),
            "sync_mode": "MANUAL",
        }

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            data_source_id, domain_id, workspace_id
        )

        if data_source_vo.state == "DISABLED":
            raise ERROR_DATA_SOURCE_STATE(data_source_id=data_source_id)

        if data_source_vo.data_source_type == "LOCAL":
            raise ERROR_NOT_ALLOW_SYNC_COMMAND(data_source_id=data_source_id)

        return job_service.create_cost_job(data_source_vo, job_options)

    @transaction(
        permission="cost-analysis:DataSource.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @convert_model
    def get(self, params: DataSourceGetRequest) -> Union[DataSourceResponse, dict]:
        """Get data source

        Args:
            params (dict): {
                'data_source_id': 'str',  # required
                'workspace_id': 'list'
                'domain_id': 'str',      # injected from auth
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params.data_source_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        data_source_vo = self.data_source_mgr.get_data_source(
            data_source_id, domain_id, workspace_id
        )

        # Check data fields permissions
        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            data_source_vo = (
                self._filter_cost_data_keys_with_permissions_by_data_source_vo(
                    data_source_vo
                )
            )

        return DataSourceResponse(**data_source_vo.to_dict())

    @transaction(
        permission="cost-analysis:DataSource.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @append_query_filter(
        [
            "data_source_id",
            "name",
            "state",
            "data_source_type",
            "provider",
            "workspace_id",
            "domain_id",
        ]
    )
    @change_tag_filter("tags")
    @append_keyword_filter(["data_source_id", "name"])
    @convert_model
    def list(
        self, params: DataSourceSearchQueryRequest
    ) -> Union[DataSourcesResponse, dict]:
        """List data sources

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'data_source_id': 'str',
                'name': 'str',
                'state': 'str',
                'data_source_type': 'str',
                'provider': 'str',
                'connected_workspace_id': str,
                'workspace_id': 'list,
                'domain_id': 'str',

            }

        Returns:
            data_source_vos (object)
            total_count
        """

        query = params.query or {}
        connected_workspace_id = params.connected_workspace_id

        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            self._check_only_fields_for_permissions(query)

        if connected_workspace_id:
            (
                data_source_vos,
                total_count,
            ) = self._change_filter_connected_workspace_data_source(
                query, connected_workspace_id
            )
        else:
            data_source_vos, total_count = self.data_source_mgr.list_data_sources(query)

        data_sources_info = self._get_data_sources_info_by_role_type(data_source_vos)

        return DataSourcesResponse(results=data_sources_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:DataSource.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @change_tag_filter("tags")
    @append_keyword_filter(["data_source_id", "name"])
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

        query = params.get("query", {})
        return self.data_source_mgr.stat_data_sources(query)

    def validate_secret_filter(self, secret_filter, domain_id):
        if "secrets" in secret_filter:
            _query = {
                "filter": [{"k": "secret_id", "v": secret_filter["secrets"], "o": "in"}]
            }
            secret_mgr: SecretManager = self.locator.get_manager(SecretManager)
            response = secret_mgr.list_secrets(_query)
            if response.get("total_count", 0) != len(secret_filter["secrets"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.secrets", reason="Secrets not found"
                )

        if "service_accounts" in secret_filter:
            _query = {
                "filter": [
                    {
                        "k": "service_account_id",
                        "v": secret_filter["service_accounts"],
                        "o": "in",
                    }
                ]
            }
            identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
            response = identity_mgr.list_service_accounts(_query, domain_id)
            if response.get("total_count", 0) != len(secret_filter["service_accounts"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.service_accounts",
                    reason="Service accounts not found",
                )

        if "schemas" in secret_filter:
            _query = {
                "filter": [{"k": "name", "v": secret_filter["schemas"], "o": "in"}]
            }
            repo_mgr: RepositoryManager = self.locator.get_manager(RepositoryManager)
            response = repo_mgr.list_schemas(_query, domain_id)
            if response.get("total_count", 0) != len(secret_filter["schemas"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.schema", reason="Schema not found"
                )

    def _check_plugin(self, plugin_id: str) -> None:
        repo_mgr: RepositoryManager = self.locator.get_manager("RepositoryManager")
        repo_mgr.get_plugin(plugin_id)

    def _init_plugin(self, endpoint, options, domain_id):
        self.ds_plugin_mgr.initialize(endpoint)
        return self.ds_plugin_mgr.init_plugin(options, domain_id)

    def _verify_plugin(self, endpoint, plugin_info, domain_id):
        options = plugin_info.get("options", {})
        secret_id = plugin_info.get("secret_id")
        secret_data = plugin_info.get("secret_data")
        schema = plugin_info.get("schema")

        if not secret_data:
            secret_data = self._get_secret_data(secret_id, domain_id)

        self.ds_plugin_mgr.initialize(endpoint)
        self.ds_plugin_mgr.verify_plugin(options, secret_data, schema, domain_id)

    def _get_secret_data(self, secret_id, domain_id):
        secret_mgr: SecretManager = self.locator.get_manager("SecretManager")
        if secret_id:
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
        else:
            secret_data = {}

        return secret_data

    @staticmethod
    def _validate_plugin_info(plugin_info: dict, secret_type: str) -> None:
        if "plugin_id" not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key="plugin_info.plugin_id")

        if (
            plugin_info.get("upgrade_mode", "AUTO") == "MANUAL"
            and "version" not in plugin_info
        ):
            raise ERROR_REQUIRED_PARAMETER(key="plugin_info.version")

        if secret_type == "MANUAL" and plugin_info.get("secret_data") is None:
            raise ERROR_REQUIRED_PARAMETER(key="plugin_info.secret_data")

    def create_data_source_account_with_data_source_vo(
        self, accounts_info: dict, data_source_vo: DataSource
    ) -> None:
        data_source_id = data_source_vo.data_source_id
        workspace_id = data_source_vo.workspace_id
        domain_id = data_source_vo.domain_id
        resource_group = data_source_vo.resource_group

        exist_data_source_account_vo_map = self._get_data_source_account_vo_map(
            data_source_id, domain_id
        )

        create_account_count = 0

        for account_info in accounts_info.get("results", []):
            account_info.update(
                {
                    "data_source_id": data_source_id,
                    "domain_id": domain_id,
                    "is_sync": False,
                }
            )
            if resource_group == "WORKSPACE":
                account_info["workspace_id"] = workspace_id

            data_source_account_vos = (
                self.data_source_account_mgr.filter_data_source_accounts(
                    data_source_id=data_source_vo.data_source_id,
                    account_id=account_info["account_id"],
                    domain_id=domain_id,
                )
            )
            if len(data_source_account_vos) == 0:
                self.data_source_account_mgr.create_data_source_account(
                    params=account_info
                )
                create_account_count += 1
            else:
                data_source_account_vo = data_source_account_vos[0]
                if data_source_account_vo.name != account_info["name"]:
                    self.data_source_account_mgr.update_data_source_account_by_vo(
                        {"name": account_info["name"]}, data_source_account_vo
                    )

            # Remove account from map if account is exist or updated
            if exist_data_source_account_vo_map.get(account_info["account_id"]):
                exist_data_source_account_vo_map.pop(account_info["account_id"])

        _LOGGER.debug(
            f"[register] create data source account: {data_source_vo.data_source_id} / total count = {create_account_count}"
        )

        # Delete old data source accounts
        for data_source_account_vo in exist_data_source_account_vo_map.values():
            self.data_source_account_mgr.delete_source_account_by_vo(
                data_source_account_vo
            )

    def _get_data_source_account_vo_map(
        self, data_source_id: str, domain_id: str
    ) -> dict:
        data_source_account_vo_map = {}
        data_source_account_vos = (
            self.data_source_account_mgr.filter_data_source_accounts(
                data_source_id=data_source_id, domain_id=domain_id
            )
        )
        for data_source_account_vo in data_source_account_vos:
            data_source_account_vo_map[data_source_account_vo.account_id] = (
                data_source_account_vo
            )
        return data_source_account_vo_map

    def _change_filter_connected_workspace_data_source(
        self, query: dict, connected_workspace_id: str
    ) -> Tuple[Union[QuerySet, list], int]:
        connected_data_source_ids = []
        domain_id = self._get_domain_id_from_filter(query)
        workspace_ids = self._get_workspace_ids_from_filter(query)

        # Check permission for connected_workspace_id
        if len(workspace_ids) > 0 and connected_workspace_id not in workspace_ids:
            return [], 0

        copied_query = self._get_copied_remove_only_filter_query(query)
        data_source_vos, _ = self.data_source_mgr.list_data_sources(copied_query)

        for data_source_vo in data_source_vos:
            resource_group = data_source_vo.resource_group
            if resource_group == "DOMAIN":
                plugin_info_metadata = data_source_vo.plugin_info.metadata
                use_account_routing = plugin_info_metadata.get(
                    "use_account_routing", False
                )
                if use_account_routing:
                    data_source_account_vos = (
                        self.data_source_account_mgr.filter_data_source_accounts(
                            domain_id=domain_id, workspace_id=connected_workspace_id
                        )
                    )
                    if len(data_source_account_vos) > 0:
                        connected_data_source_ids.append(data_source_vo.data_source_id)
                else:
                    connected_data_source_ids.append(data_source_vo.data_source_id)
            else:
                connected_data_source_ids.append(data_source_vo.data_source_id)

        if connected_data_source_ids:
            query["filter"] = [
                {"k": "data_source_id", "v": connected_data_source_ids, "o": "in"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]

        _LOGGER.debug(
            f"[_change_filter_connected_workspace_data_source] query: {query}"
        )

        return self.data_source_mgr.list_data_sources(query)

    def _get_data_sources_info_by_role_type(self, data_source_vos: list) -> list:
        data_sources_info = []
        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            for data_source_vo in data_source_vos:
                data_source_vo = (
                    self._filter_cost_data_keys_with_permissions_by_data_source_vo(
                        data_source_vo
                    )
                )
                data_sources_info.append(data_source_vo.to_dict())
        else:
            for data_source_vo in data_source_vos:
                data_sources_info.append(data_source_vo.to_dict())

        return data_sources_info

    @staticmethod
    def _get_domain_id_from_filter(query: dict) -> str:
        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))

            if key == "domain_id":
                return value

    @staticmethod
    def _get_workspace_ids_from_filter(query: dict) -> list:
        workspace_ids = []
        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))

            if key == "workspace_id":
                if isinstance(value, list):
                    workspace_ids.extend(value)
                else:
                    workspace_ids.append(value)
        return workspace_ids

    @staticmethod
    def _get_copied_remove_only_filter_query(query: dict) -> dict:
        copied_query = copy.deepcopy(query)
        copied_query.pop("only", None)
        copied_query.pop("minimal", None)
        _LOGGER.debug(f"[_get_copied_remove_only_filter_query] query: {copied_query}")
        return copied_query

    @staticmethod
    def _filter_cost_data_keys_with_permissions_by_data_source_vo(
        data_source_vo: DataSource,
    ) -> DataSource:
        if data_source_vo.permissions:
            deny = data_source_vo.permissions.get("deny", [])
            cost_data_keys = data_source_vo.cost_data_keys or []
            for deny_key in deny:
                if deny_key.startswith("data."):
                    split_denied_key = deny_key.split(".")[1]
                    if split_denied_key in cost_data_keys:
                        cost_data_keys.remove(split_denied_key)
            data_source_vo.cost_data_keys = cost_data_keys
        return data_source_vo

    @staticmethod
    def _check_only_fields_for_permissions(query: dict) -> None:
        only_fields = query.get("only", [])
        for only_field in only_fields:
            if only_field == "cost_data_keys":
                if "permissions" not in only_fields:
                    raise ERROR_INVALID_PARAMETER(
                        key="permissions",
                        reason="when you want to get 'cost_data_keys', you must include 'permissions' field in 'only' field.",
                    )
