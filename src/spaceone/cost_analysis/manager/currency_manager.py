import logging
from datetime import datetime

from typing import Tuple

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.currency_connector import CurrencyConnector

_LOGGER = logging.getLogger(__name__)


class CurrencyManager(BaseManager):
    def __init__(self, *args, today: datetime = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.currency_connector: CurrencyConnector = CurrencyConnector(today=today)
        self.currency_mapper = {}

    def get_currency_map_date(
        self, currency_end_date: datetime, currency_start_date: datetime = None
    ) -> Tuple[dict, str]:
        currency_map, currency_date = self.currency_connector.add_currency_map_date(
            currency_end_date=currency_end_date, currency_start_date=currency_start_date
        )

        return currency_map, currency_date
