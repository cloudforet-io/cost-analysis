import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.budget_usage_model import BudgetUsage

_LOGGER = logging.getLogger(__name__)


class BudgetUsageManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_usage_model: BudgetUsage = self.locator.get_model('BudgetUsage')

    def list_budget_usages(self, query={}):
        return self.budget_usage_model.query(**query)

    def stat_budget_usages(self, query):
        return self.budget_usage_model.stat(**query)
