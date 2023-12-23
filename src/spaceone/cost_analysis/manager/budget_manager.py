import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.budget_model import Budget

_LOGGER = logging.getLogger(__name__)


class BudgetManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_model: Budget = self.locator.get_model("Budget")

    def create_budget(self, params: dict) -> Budget:
        def _rollback(vo: Budget):
            _LOGGER.info(
                f"[create_budget._rollback] "
                f"Delete budget : {vo.name} "
                f"({vo.budget_id})"
            )
            vo.delete()

        budget_vo: Budget = self.budget_model.create(params)
        self.transaction.add_rollback(_rollback, budget_vo)

        return budget_vo

    def update_budget_by_vo(self, params, budget_vo):
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_budget_by_vo._rollback] Revert Data : "
                f'{old_data["budget_id"]}'
            )
            budget_vo.update(old_data)

        self.transaction.add_rollback(_rollback, budget_vo.to_dict())
        return budget_vo.update(params)

    @staticmethod
    def delete_budget_by_vo(budget_vo: Budget) -> None:
        budget_vo.delete()

    def get_budget(
        self, budget_id: str, domain_id: str, workspace_id: str = None, project_id=None
    ) -> Budget:
        conditions = {"budget_id": budget_id, "domain_id": domain_id}

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if project_id:
            conditions["project_id"] = project_id

        return self.budget_model.get(**conditions)

    def filter_budgets(self, **conditions):
        return self.budget_model.filter(**conditions)

    def list_budgets(self, query: dict):
        return self.budget_model.query(**query)

    def stat_budgets(self, query):
        return self.budget_model.stat(**query)
