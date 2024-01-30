import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.currency_connector import CurrencyConnector

_LOGGER = logging.getLogger(__name__)


class CurrencyManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.currency_connector: CurrencyConnector = CurrencyConnector()

    def convert_exchange_rate(self, aggregated_cost_report: dict) -> dict:
        return self.currency_connector.add_exchange_rate(aggregated_cost_report)
