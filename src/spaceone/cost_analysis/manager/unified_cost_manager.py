import logging
from typing import Tuple, Union
from mongoengine import QuerySet

from spaceone.core import queue, utils, config
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.unified_cost.database import UnifiedCost

_LOGGER = logging.getLogger(__name__)


class UnifiedCostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unified_cost_model = UnifiedCost

    def create_unified_cost(self, params: dict) -> UnifiedCost:
        def _rollback(vo: UnifiedCost):
            _LOGGER.info(
                f"[create_unified_cost._rollback] Delete unified_cost : {vo.unified_cost_id}, {vo.to_dict()} "
            )
            vo.delete()

        unified_cost_vo: UnifiedCost = self.unified_cost_model.create(params)
        self.transaction.add_rollback(_rollback, unified_cost_vo)

        return unified_cost_vo

    @staticmethod
    def delete_unified_cost_by_vo(unified_cost_vo: UnifiedCost) -> None:
        unified_cost_vo.delete()

    def get_unified_cost(
        self,
        unified_cost_id: str,
        domain_id: str,
        workspace_id: str = None,
        project_id: Union[list, str] = None,
    ) -> UnifiedCost:
        conditions = {
            "unified_cost_id": unified_cost_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if project_id:
            conditions["project_id"] = project_id

        return self.unified_cost_model.get(**conditions)

    def filter_unified_costs(self, **conditions) -> QuerySet:
        return self.unified_cost_model.filter(**conditions)

    def list_unified_costs(self, query: dict) -> Tuple[QuerySet, int]:
        return self.unified_cost_model.query(**query)

    def analyze_unified_costs(self, query: dict, target="SECONDARY_PREFERRED") -> dict:
        query["target"] = target
        query["date_field"] = "billed_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_unified_costs] query: {query}")

        return self.unified_cost_model.analyze(**query)

    def stat_unified_costs(self, query) -> dict:
        return self.unified_cost_model.stat(**query)

    @staticmethod
    def push_unified_cost_job_task(params: dict) -> None:
        token = config.get_global("TOKEN")
        task = {
            "name": "run_unified_cost",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                {
                    "locator": "SERVICE",
                    "name": "UnifiedCostService",
                    "metadata": {"token": token},
                    "method": "run_unified_cost",
                    "params": {"params": params},
                }
            ],
        }

        _LOGGER.debug(f"[push_job_task] task param: {params}")

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
