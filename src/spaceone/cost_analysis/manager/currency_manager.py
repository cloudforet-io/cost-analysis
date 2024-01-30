import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.currency_connector import CurrencyConnector

_LOGGER = logging.getLogger(__name__)


class CurrencyManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.currency_connector: CurrencyConnector = CurrencyConnector()
        self.currency_mapper = {}

    def get_currency_map_date(self, to_currency: str) -> tuple[dict, str]:
        currency_map, currency_date = self.currency_connector.add_currency_map_date(
            to_currency
        )

        return currency_map, currency_date
