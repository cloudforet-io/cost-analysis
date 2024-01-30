import logging
import FinanceDataReader as fdr

from datetime import datetime, timedelta

from spaceone.core.connector import BaseConnector

__all__ = ["CurrencyConnector"]

_LOGGER = logging.getLogger(__name__)

EXCHANGE_CURRENCY_LIST = ["KRW", "USD", "JPY"]


class CurrencyConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.today = datetime.utcnow()
        self.today_date = self.today.strftime("%Y-%m-%d")
        self.two_weeks_ago = (self.today - timedelta(days=14)).strftime("%Y-%m-%d")

    def add_exchange_rate(self, aggregated_cost_report: dict) -> dict:
        current_currency_dict = aggregated_cost_report.get("cost")
        cost = {}

        for current_currency, current_cost in current_currency_dict.items():
            exchange_rate_cost = self._calculate_exchange_rate(
                current_currency, current_cost
            )
            cost.update({current_currency: exchange_rate_cost})

        aggregated_cost_report.update({"cost": cost})
        return aggregated_cost_report

    def _calculate_exchange_rate(self, from_currency: str, amount: float) -> float:
        exchange_rates = {}

        for to_currency in EXCHANGE_CURRENCY_LIST:
            if from_currency == to_currency:
                exchange_rate = 1.0
            else:
                pair = f"{from_currency}/{to_currency}"
                exchange_rate_info = fdr.DataReader(
                    pair, self.two_weeks_ago, self.today_date
                )["Close"].dropna()
                exchange_rate = exchange_rate_info.iloc[-1]

            exchange_rates[to_currency] = exchange_rate

        exchange_rate_cost = amount * exchange_rates[from_currency]
        return exchange_rate_cost
