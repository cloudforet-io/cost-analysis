import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.cost_query_set_model import CostQuerySet

_LOGGER = logging.getLogger(__name__)


class CostQuerySetManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_query_set_model: CostQuerySet = self.locator.get_model("CostQuerySet")

    def create_cost_query_set(self, params: dict):
        def _rollback(vo: CostQuerySet):
            _LOGGER.info(
                f"[create_cost_query_set._rollback] "
                f"Delete cost_query_set : {cost_query_set_vo.name} "
                f"({vo.cost_query_set_id})"
            )
            vo.delete()

        cost_query_set_vo: CostQuerySet = self.cost_query_set_model.create(params)
        self.transaction.add_rollback(_rollback, cost_query_set_vo)

        return cost_query_set_vo

    def update_cost_query_set(self, params):
        cost_query_set_vo: CostQuerySet = self.get_cost_query_set(
            params["cost_query_set_id"], params["domain_id"]
        )
        return self.update_cost_query_set_by_vo(params, cost_query_set_vo)

    def update_cost_query_set_by_vo(self, params, cost_query_set_vo):
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_cost_query_set_by_vo._rollback] Revert Data : "
                f'{old_data["cost_query_set_id"]}'
            )
            cost_query_set_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cost_query_set_vo.to_dict())
        return cost_query_set_vo.update(params)

    @staticmethod
    def delete_cost_query_set_by_vo(cost_query_set_vo: CostQuerySet) -> None:
        cost_query_set_vo.delete()

    def get_cost_query_set(
        self,
        cost_query_set_id: str,
        user_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> CostQuerySet:
        conditions = {
            "cost_query_set_id": cost_query_set_id,
            "user_id": user_id,
            "domain_id": domain_id,
        }
        if workspace_id:
            conditions["workspace_id"] = workspace_id
        return self.cost_query_set_model.get(**conditions)

    def list_cost_query_sets(self, query={}):
        return self.cost_query_set_model.query(**query)

    def stat_cost_query_sets(self, query):
        return self.cost_query_set_model.stat(**query)
