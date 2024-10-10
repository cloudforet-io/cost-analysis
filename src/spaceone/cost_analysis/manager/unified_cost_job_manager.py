import logging
from datetime import datetime
from mongoengine import QuerySet
from typing import Tuple

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.unified_cost.database import UnifiedCostJob

_LOGGER = logging.getLogger(__name__)


class UnifiedCostJobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unified_cost_job_model = UnifiedCostJob

    def create_unified_cost(self, params: dict):
        def _rollback(vo: UnifiedCostJob):
            _LOGGER.info(
                f"[create_unified_cost_job._rollback] Delete unified_cost : {vo.unified_cost_job_id}"
            )
            vo.delete()

        params["is_confirmed"] = False

        unified_cost_job_vo: UnifiedCostJob = self.unified_cost_job_model.create(params)
        self.transaction.add_rollback(_rollback, unified_cost_job_vo)

        return unified_cost_job_vo

    @staticmethod
    def delete_unified_cost_job_by_vo(unified_cost_job_vo: UnifiedCostJob):
        unified_cost_job_vo.delete()

    def get_unified_cost(
        self,
        billed_month: str,
        domain_id: str,
    ) -> UnifiedCostJob:
        return self.unified_cost_job_model.get(
            domain_id=domain_id, billed_month=billed_month
        )

    def update_unified_cost_job_by_vo(
        self, params: dict, unified_cost_job_vo: UnifiedCostJob
    ) -> UnifiedCostJob:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_cost_report_config_by_vo._rollback] Revert Data: {old_data['cost_report_config_id']}"
            )
            unified_cost_job_vo.update(old_data)

        self.transaction.add_rollback(_rollback, unified_cost_job_vo.to_dict())

        return unified_cost_job_vo.update(params)

    @staticmethod
    def update_is_confirmed_unified_cost_job(
        unified_cost_job_vo: UnifiedCostJob, is_confirmed: bool
    ):
        update_params = {"is_confirmed": is_confirmed}

        if is_confirmed:
            update_params["confirmed_at"] = datetime.utcnow()
        return unified_cost_job_vo.update(update_params)

    def filter_unified_cost_jobs(self, **conditions):
        return self.unified_cost_job_model.filter(**conditions)

    def list_unified_costs(self, query: dict) -> Tuple[QuerySet, int]:
        return self.unified_cost_job_model.query(**query)
