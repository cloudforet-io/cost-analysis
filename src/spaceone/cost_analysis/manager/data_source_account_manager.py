import logging
from typing import Tuple, Union

from mongoengine import QuerySet
from spaceone.core import utils
from spaceone.core.manager import BaseManager

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

    def analyze_data_source_accounts(self, query: dict) -> dict:
        return self.data_source_account_model.analyze(**query)

    def stat_data_source_accounts(self, query: dict) -> dict:
        return self.data_source_account_model.stat(**query)

    def get_workspace_id_from_account_id(
        self, cost_data: dict, domain_id: str, data_source_id: str
    ) -> Tuple[str, str]:
        workspace_id = None
        v_workspace_id = None

        data_source_vo = self._get_data_source(data_source_id, domain_id)
        plugin_info_metadata = data_source_vo.plugin_info.metadata

        use_account_routing = plugin_info_metadata.get("use_account_routing", False)

        if use_account_routing:
            account_match_key = plugin_info_metadata.get("account_match_key")
            account_match_key_value = utils.get_dict_value(cost_data, account_match_key)

            if account_match_key_value:
                ds_account_vos = self.filter_data_source_accounts(
                    data_source_id=data_source_id,
                    account_id=account_match_key_value,
                    domain_id=domain_id,
                )
                if ds_account_vos.count() > 0:
                    ds_account_vo = ds_account_vos[0]
                    workspace_id = ds_account_vo.workspace_id
                    v_workspace_id = ds_account_vo.v_workspace_id

        return workspace_id, v_workspace_id

    def connect_account_by_data_source_vo(
        self,
        data_source_account_vo: DataSourceAccount,
        data_source_vo: DataSource,
    ) -> DataSourceAccount:
        domain_id = data_source_vo.domain_id

        reference_id = data_source_account_vo.account_id
        workspace_info = self._get_workspace_by_references(reference_id, domain_id)
        if workspace_info:
            data_source_account_vo = self.update_data_source_account_by_vo(
                {"workspace_id": workspace_info.get("workspace_id"), "is_linked": True},
                data_source_account_vo,
            )

        return data_source_account_vo

    def _get_workspace_by_references(
        self, reference_id: str, domain_id: str
    ) -> Union[dict, None]:
        if f"workspace:{domain_id}:references:{reference_id}" in self._workspace_info:
            return self._workspace_info[
                f"workspace:{domain_id}:references:{reference_id}"
            ]

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "references", "v": [reference_id], "o": "in"},
            ]
        }

        identity_mgr = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_workspaces({"query": query}, domain_id)
        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        workspace_info = None

        if total_count == 0:
            response = identity_mgr.list_workspaces(
                {"query": {"filter": [{"k": "domain_id", "v": domain_id, "o": "eq"}]}},
                domain_id,
            )
            total_count = response.get("total_count", 0)
            if total_count == 1:
                workspace_info = response.get("results")[0]

        else:
            workspace_info = results[0]

        self._workspace_info[
            f"workspace:{domain_id}:references:{reference_id}"
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
