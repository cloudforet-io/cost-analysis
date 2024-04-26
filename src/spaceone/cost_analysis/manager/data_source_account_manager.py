import logging
from typing import Tuple, Union

from mongoengine import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils

from spaceone.cost_analysis.model import DataSource
from spaceone.cost_analysis.model.data_source_account.database import DataSourceAccount

_LOGGER = logging.getLogger(__name__)


class DataSourceAccountManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr = self.locator.get_manager("DataSourceManager")
        self.data_source_account_model = DataSourceAccount
        self._workspace_info = {}
        self._data_source_info = {}

    def create_data_source_account(self, params: dict) -> DataSourceAccount:
        def _rollback(vo: DataSourceAccount):
            _LOGGER.info(
                f"[create_cost_report._rollback] Delete data_source_account: {vo.account_id} {vo.data_source_id})"
            )
            vo.delete()

        data_source_account_vo = self.data_source_account_model.create(params)
        self.transaction.add_rollback(_rollback, data_source_account_vo)

        return data_source_account_vo

    def update_data_source_account_by_vo(
        self, params: dict, data_source_account_vo: DataSourceAccount
    ) -> DataSourceAccount:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_data_source_account_by_vo._rollback] Revert Data: {old_data['account_id']} {old_data['data_source_id']}"
            )
            data_source_account_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_source_account_vo.to_dict())

        return data_source_account_vo.update(params)

    @staticmethod
    def delete_source_account_by_vo(
        data_source_account_vo: DataSourceAccount,
    ) -> None:
        data_source_account_vo.delete()

    def get_data_source_account(
        self,
        data_source_id: str,
        account_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> DataSourceAccount:
        conditions = {
            "data_source_id": data_source_id,
            "account_id": account_id,
            "domain_id": domain_id,
        }
        if workspace_id:
            conditions["workspace_id"] = workspace_id

        return self.data_source_account_model.get(**conditions)

    def delete_ds_account_with_data_source(self, data_source_id: str, domain_id: str):
        _LOGGER.debug(f"[delete_cost_with_datasource] data_source_id: {data_source_id}")
        data_source_account_vos = self.filter_data_source_accounts(
            data_source_id=data_source_id, domain_id=domain_id
        )
        data_source_account_vos.delete()

    def list_data_source_accounts(self, query: dict) -> Tuple[QuerySet, int]:
        return self.data_source_account_model.query(**query)

    def filter_data_source_accounts(self, **conditions) -> QuerySet:
        return self.data_source_account_model.filter(**conditions)

    def stat_data_source_accounts(self, query: dict) -> dict:
        return self.data_source_account_model.stat(**query)

    def connect_cost_data(
        self, cost_data: dict
    ) -> Tuple[dict, Union[DataSourceAccount, None]]:
        data_source_id = cost_data["data_source_id"]
        domain_id = cost_data["domain_id"]

        data_source_vo = self._get_data_source(data_source_id, domain_id)
        plugin_info_metadata = data_source_vo.plugin_info.metadata

        use_account_routing = plugin_info_metadata.get("use_account_routing", False)

        ds_account_vo = None
        if use_account_routing:
            account_connect_polices: list = plugin_info_metadata.get(
                "account_connect_polices"
            )
            for account_connect_policy in account_connect_polices:
                if account_connect_policy.get("name") == "connect_cost_to_account":
                    name = account_connect_policy.get("name")
                    policy = account_connect_policy["polices"].get(name)
                    source = policy["source"]
                    target_key = policy.get("target", "account_id")
                    target_value = utils.get_dict_value(cost_data, source)
                    operator = policy.get("operator")

                    if target_value:
                        ds_account_vo = self._get_data_source_account_vo(
                            target_key,
                            target_value,
                            data_source_id,
                            domain_id,
                            operator,
                        )

        if ds_account_vo:
            cost_data["account_id"] = ds_account_vo.account_id
            cost_data["workspace_id"] = ds_account_vo.v_workspace_id

        return cost_data, ds_account_vo

    def connect_account_by_data_source_vo(
        self,
        data_source_account_vo: DataSourceAccount,
        data_source_vo: DataSource,
    ) -> DataSourceAccount:
        domain_id = data_source_vo.domain_id

        plugin_info_metadata = data_source_vo.plugin_info.metadata
        account_connect_polices: list = plugin_info_metadata.get(
            "account_connect_polices"
        )

        for account_connect_policy in account_connect_polices:
            if account_connect_policy.get("name") == "connect_account_to_workspace":
                name = account_connect_policy.get("name")
                policy = account_connect_policy["polices"].get(name)
                source = policy["source"]
                target_key = policy.get("target", "references")

                target_value = utils.get_dict_value(
                    data_source_account_vo.to_dict(),
                    source,
                )
                operator = policy.get("operator")

                if target_value:
                    workspace_info = self._get_workspace(
                        target_key, target_value, domain_id, operator
                    )

                    if workspace_info:
                        data_source_account_vo = self.update_data_source_account_by_vo(
                            {"workspace_id": workspace_info.get("workspace_id")},
                            data_source_account_vo,
                        )
        return data_source_account_vo

    def _get_workspace(
        self, target_key: str, target_value: str, domain_id: str, operator: str = "eq"
    ) -> Union[dict, None]:
        if f"workspace:{domain_id}:{target_key}:{target_value}" in self._workspace_info:
            return self._workspace_info[
                f"workspace:{domain_id}:{target_key}:{target_value}"
            ]

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }
        if operator == "in" and not isinstance(target_value, list):
            target_value = [target_value]
        query["filter"].append({"k": target_key, "v": target_value, "o": operator})

        identity_mgr = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_workspaces({"query": query}, domain_id)
        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        workspace_info = None
        if total_count > 0:
            workspace_info = results[0]

        self._workspace_info[
            f"workspace:{domain_id}:{target_key}:{target_value}"
        ] = workspace_info

        return workspace_info

    def _get_data_source(self, data_source_id: str, domain_id: str) -> DataSource:
        if f"data-source:{domain_id}:{data_source_id}" in self._data_source_info:
            return self._data_source_info[f"data-source:{domain_id}:{data_source_id}"]

        data_source_vo = self.data_source_mgr.get_data_source(
            data_source_id=data_source_id, domain_id=domain_id
        )
        self._data_source_info[
            f"data-source:{domain_id}:{data_source_id}"
        ] = data_source_vo

        return data_source_vo

    def _get_data_source_account_vo(
        self,
        target_key: str,
        target_value: str,
        data_source_id: str,
        domain_id: str,
        operator: str = "eq",
    ) -> Union[DataSourceAccount, None]:
        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": target_key, "v": target_value, "o": operator},
            ]
        }

        data_source_account_vos, total_count = self.list_data_source_accounts(query)
        data_source_account_vo = None
        if total_count > 0:
            data_source_account_vo = data_source_account_vos[0]

        return data_source_account_vo
