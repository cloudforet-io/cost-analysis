import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.cost_report_data.database import CostReportData

_LOGGER = logging.getLogger(__name__)


class CostReportDataManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_data_model = CostReportData

    def create_cost_report_data(self, params: dict) -> CostReportData:
        def _rollback(vo: CostReportData):
            _LOGGER.info(
                f"[create_cost_report_data._rollback] Delete cost report data : {vo.cost_report_id}, {vo.cost_report_data_id} "
            )
            vo.delete()

        cost_report_data_vo: CostReportData = self.cost_report_data_model.create(params)
        self.transaction.add_rollback(_rollback, cost_report_data_vo)

        return cost_report_data_vo

    def update_cost_report_data_by_vo(
        self, params: dict, cost_report_data_vo: CostReportData
    ) -> CostReportData:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_cost_report_data_by_vo._rollback] Revert Data : "
                f'{old_data["cost_report_data_id"]}'
            )
            cost_report_data_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cost_report_data_vo.to_dict())
        return cost_report_data_vo.update(params)

    @staticmethod
    def delete_cost_report_data(cost_report_data_vo: CostReportData):
        cost_report_data_vo.delete()

    def get_cost_report_data(
        self, cost_report_data_id: str, domain_id: str, workspace_id: str = None
    ):
        conditions = {
            "cost_report_data_id": cost_report_data_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id
        return self.cost_report_data_model.get(**conditions)

    def filter_cost_reports_data(self, **conditions):
        return self.cost_report_data_model.filter(**conditions)

    def list_cost_reports_data(self, query: dict) -> Tuple[QuerySet, int]:
        return self.cost_report_data_model.query(**query)

    def analyze_cost_reports_data(
        self, query: dict, target="SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "report_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_cost_reports_data] query: {query}")

        return self.cost_report_data_model.analyze(**query)

    def stat_cost_reports_data(self, query) -> dict:
        return self.cost_report_data_model.stat(**query)

    @staticmethod
    def _extract_cost_by_currency(unified_cost):
        return {
            currency.replace("cost_", ""): value
            for currency, value in unified_cost.items()
            if currency.startswith("cost_")
        }
