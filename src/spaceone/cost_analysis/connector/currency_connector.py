import logging
import FinanceDataReader as fdr

from datetime import datetime, timedelta
from typing import Tuple, Union

from dateutil.relativedelta import relativedelta
from spaceone.core.connector import BaseConnector

__all__ = ["CurrencyConnector"]

_LOGGER = logging.getLogger(__name__)

FROM_EXCHANGE_CURRENCIES = ["KRW", "USD", "JPY"]


class CurrencyConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_currency_map_date(
        self, currency_end_date: datetime, currency_start_date: datetime = None
    ) -> Tuple[dict, str]:
        currency_map = self._initialize_currency_map()
        currency_date = currency_end_date

        for from_currency in FROM_EXCHANGE_CURRENCIES:
            for to_currency in FROM_EXCHANGE_CURRENCIES:
                if from_currency == to_currency:
                    exchange_rate = 1.0
                else:
                    pair = f"{from_currency}/{to_currency}"
                    exchange_rate_info = self._get_exchange_rate_info(
                        pair=pair,
                        currency_end_date=currency_end_date,
                        currency_start_date=currency_start_date,
                    )

                    currency_date, exchange_rate = exchange_rate_info.iloc[-1]
                currency_map[from_currency][
                    f"{from_currency}/{to_currency}"
                ] = exchange_rate

        return currency_map, currency_date

    @staticmethod
    def _initialize_currency_map():
        currency_map = {}
        for exchange_currency in FROM_EXCHANGE_CURRENCIES:
            currency_map[exchange_currency] = {}
        return currency_map

    @staticmethod
    def _get_exchange_rate_info(
        pair: str,
        currency_end_date: datetime,
        currency_start_date: Union[datetime, None] = None,
    ):
        if not currency_start_date:
            currency_start_date = currency_end_date - relativedelta(days=14)

        return (
            fdr.DataReader(
                symbol=pair,
                start=currency_start_date,
                end=currency_end_date,
            )
            .dropna()
            .reset_index()[["Date", "Close"]]
        )
