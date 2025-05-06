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

    def get_monthly_total_cost_for_adjustments(
        self,
        cost_report_id: str,
        report_month: str,
        domain_id: str,
        workspace_id: str,
        project_id: str = None,
        target="SECONDARY_PREFERRED",
    ) -> dict:
        currencies = ["KRW", "USD", "JPY"]

        query = {
            # "group_by": ["workspace_id"],
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "cost_report_id", "v": cost_report_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                {"k": "report_year", "v": report_month.split("-")[0], "o": "eq"},
                {"k": "report_month", "v": report_month, "o": "eq"},
            ],
            "target": target,
            "date_field": "report_month",
            "date_field_format": "%Y-%m",
        }

        fields = {
            f"cost_{currency}": {"key": f"cost.{currency}", "operator": "sum"}
            for currency in currencies
        }
        query["fields"] = fields

        if project_id:
            query["group_by"] = ["project_id"]
            query["filter"].append({"k": "project_id", "v": project_id, "o": "eq"})

        _LOGGER.debug(f"[analyze_cost_reports_data] query: {query}")

        results = self.cost_report_data_model.analyze(**query)
        costs = results.get("results")[0]
        costs = self._extract_cost_by_currency(costs)
        return costs

    def stat_cost_reports_data(self, query) -> dict:
        return self.cost_report_data_model.stat(**query)

    @staticmethod
    def _extract_cost_by_currency(unified_cost):
        return {
            currency.replace("cost_", ""): value
            for currency, value in unified_cost.items()
            if currency.startswith("cost_")
        }
