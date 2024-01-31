import logging
import FinanceDataReader as fdr

from datetime import datetime, timedelta
from typing import Tuple

from spaceone.core.connector import BaseConnector

__all__ = ["CurrencyConnector"]

_LOGGER = logging.getLogger(__name__)

FROM_EXCHANGE_CURRENCIES = ["KRW", "USD", "JPY"]


class CurrencyConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.today = datetime.utcnow()
        self.today_date = self.today.strftime("%Y-%m-%d")
        self.two_weeks_ago = (self.today - timedelta(days=14)).strftime("%Y-%m-%d")
        self.currency_map = None
        self.currency_date = None

    def add_currency_map_date(self) -> Tuple[dict, str]:
        currency_map = self._initialize_currency_map()
        _currency_date = ""

        for from_currency in FROM_EXCHANGE_CURRENCIES:
            for to_currency in FROM_EXCHANGE_CURRENCIES:
                if from_currency == to_currency:
                    exchange_rate = 1.0
                else:
                    pair = f"{from_currency}/{to_currency}"
                    exchange_rate_info = self._get_exchange_rate_info(pair)
                    _currency_date, exchange_rate = exchange_rate_info.iloc[-1]
                currency_map[from_currency][
                    f"{from_currency}/{to_currency}"
                ] = exchange_rate

        if self.currency_date is None:
            self.currency_date = _currency_date

        return currency_map, self.currency_date

    @staticmethod
    def _initialize_currency_map():
        return {"KRW": {}, "USD": {}, "JPY": {}}

    def _get_exchange_rate_info(self, pair):
        return (
            fdr.DataReader(pair, self.two_weeks_ago, self.today_date)
            .dropna()
            .reset_index()[["Date", "Close"]]
        )
