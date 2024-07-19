import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core import queue, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.cost_report.database import CostReport

_LOGGER = logging.getLogger(__name__)


class CostReportManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_model = CostReport

    def create_cost_report(self, params: dict) -> CostReport:
        def _rollback(vo: CostReport):
            _LOGGER.info(
                f"[create_cost_report._rollback] Delete cost_report: {vo.cost_report_id})"
            )
            vo.delete()

        cost_report_vo = self.cost_report_model.create(params)
        self.transaction.add_rollback(_rollback, cost_report_vo)

        return cost_report_vo

    def get_cost_report(
        self, domain_id: str, cost_report_id: str, workspace_id: str = None
    ) -> CostReport:
        conditions = {
            "cost_report_id": cost_report_id,
            "domain_id": domain_id,
        }
        if workspace_id:
            conditions["workspace_id"] = workspace_id

        return self.cost_report_model.get(**conditions)

    def filter_cost_reports(self, **conditions) -> QuerySet:
        return self.cost_report_model.filter(**conditions)

    def list_cost_reports(self, query: dict) -> Tuple[QuerySet, int]:
        _LOGGER.debug(f"[list_cost_reports] query: {query}")
        return self.cost_report_model.query(**query)

    def stat_cost_reports(self, query: dict) -> dict:
        return self.cost_report_model.stat(**query)

    def push_creating_cost_report_job(self, params: dict) -> None:
        token = self.transaction.meta.get("token")
        task = {
            "name": "create_cost_report_job",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                {
                    "locator": "SERVICE",
                    "name": "CostReportService",
                    "metadata": {"token": token},
                    "method": "create_cost_report_by_cost_report_config_id",
                    "params": {"params": params},
                }
            ],
        }

        _LOGGER.debug(f"[push_creating_cost_report_job] task param: {params}")

        queue.put("cost_analysis_q", utils.dump_json(task))

    @staticmethod
    def get_exchange_currency(cost: float, currency: str, currency_map: dict) -> dict:
        cost_info = {}
        for convert_currency in currency_map.keys():
            cost_info.update(
                {
                    convert_currency: currency_map[currency][
                        f"{currency}/{convert_currency}"
                    ]
                    * cost
                }
            )

        return cost_info

    @staticmethod
    def get_currency_date(currency_date: str) -> str:
        currency_date = str(currency_date).split()[0]

        return currency_date
