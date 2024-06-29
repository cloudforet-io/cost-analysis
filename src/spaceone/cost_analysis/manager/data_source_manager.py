import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.data_source_model import DataSource
from spaceone.cost_analysis.model.data_source_account.database import DataSourceAccount

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_model: DataSource = self.locator.get_model("DataSource")
        self.data_source_account_model = DataSourceAccount()

    def register_data_source(self, params):
        def _rollback(data_source_vo):
            _LOGGER.info(
                f"[register_data_source._rollback] "
                f"Delete data source : {data_source_vo.name} "
                f"({data_source_vo.data_source_id})"
            )
            data_source_vo.delete()

        data_source_vo: DataSource = self.data_source_model.create(params)
        self.transaction.add_rollback(_rollback, data_source_vo)

        return data_source_vo

    def update_data_source_by_vo(
        self, params, data_source_vo: DataSource
    ) -> DataSource:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_data_source_by_vo._rollback] Revert Data : "
                f'{old_data["data_source_id"]}'
            )
            data_source_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_source_vo.to_dict())
        return data_source_vo.update(params)

    def update_data_source_account_and_connected_workspace_count_by_vo(
        self, data_source_vo: DataSource
    ) -> DataSource:
        connected_workspaces = []
        conditions = {
            "data_source_id": data_source_vo.data_source_id,
            "domain_id": data_source_vo.domain_id,
        }
        if data_source_vo.resource_group == "WORKSPACE":
            conditions["workspace_id"] = data_source_vo.workspace_id

        ds_account_vos = self.data_source_account_model.filter(**conditions)

        for ds_account_vo in ds_account_vos:
            if ds_account_vo.workspace_id:
                connected_workspaces.append(ds_account_vo.workspace_id)

        data_source_vo = self.update_data_source_by_vo(
            {
                "data_source_account_count": ds_account_vos.count(),
                "connected_workspace_count": len(set(connected_workspaces)),
            },
            data_source_vo,
        )

        _LOGGER.debug(
            f"[update_data_source_account_and_connected_workspace_count_by_vo] data_source_account_count: {data_source_vo.data_source_account_count}, connected_workspace_count: {data_source_vo.connected_workspace_count}"
        )
        return data_source_vo

    def deregister_data_source(self, data_source_id, domain_id):
        data_source_vo: DataSource = self.get_data_source(data_source_id, domain_id)
        data_source_vo.delete()

    @staticmethod
    def deregister_data_source_by_vo(data_source_vo):
        data_source_vo.delete()

    def get_data_source(
        self, data_source_id: str, domain_id: str, workspace_id: str = None
    ) -> DataSource:
        conditions = {"data_source_id": data_source_id, "domain_id": domain_id}

        if workspace_id:
            conditions["workspace_id"] = workspace_id
        return self.data_source_model.get(**conditions)

    def filter_data_sources(self, **conditions):
        return self.data_source_model.filter(**conditions)

    def list_data_sources(self, query={}):
        return self.data_source_model.query(**query)

    def stat_data_sources(self, query):
        return self.data_source_model.stat(**query)
