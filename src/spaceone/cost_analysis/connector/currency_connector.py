import FinanceDataReader as fdr
import logging
import pandas as pd
import requests
from datetime import datetime
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
    ) -> Tuple[dict, datetime]:
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
    def http_datareader(pair, currency_end_date, currency_start_date) -> dict:
        pair = f"{pair.replace('/','')}=X"
        start_date_time_stamp = int(currency_start_date.timestamp())
        end_date_time_stamp = int(currency_end_date.timestamp())

        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{pair}?period1={start_date_time_stamp}&period2={end_date_time_stamp}&interval=1d&events=history&includeAdjustedClose=true"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        response = requests.request(method="GET", url=url, headers=headers)
        return response.json()

    def _get_exchange_rate_info(
        self,
        pair: str,
        currency_end_date: datetime,
        currency_start_date: Union[datetime, None] = None,
    ):
        if not currency_start_date:
            currency_start_date = currency_end_date - relativedelta(days=15)
        try:
            return (
                fdr.DataReader(
                    pair,
                    start=currency_start_date,
                    end=currency_end_date,
                )
                .dropna()
                .reset_index()[["Date", "Close"]]
            )
        except Exception as e:
            _LOGGER.error(f"[get_exchange_rate_info] Error {e}")
            response_json = self.http_datareader(
                pair, currency_end_date, currency_start_date
            )

            quotes = response_json["chart"]["result"][0]["indicators"]["quote"][0]
            timestamps = response_json["chart"]["result"][0]["timestamp"]

            # convert bst to utc
            converted_datetime = [
                datetime.utcfromtimestamp(ts + 3600) for ts in timestamps
            ]

            df = pd.DataFrame(
                {
                    "Date": converted_datetime,
                    "Close": quotes["close"],
                }
            )

            return df.dropna().reset_index()[["Date", "Close"]]
