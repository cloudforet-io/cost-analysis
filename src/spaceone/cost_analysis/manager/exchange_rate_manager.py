import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.exchange_rate_model import ExchangeRate

_LOGGER = logging.getLogger(__name__)


class ExchangeRateManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exchange_rate_model: ExchangeRate = self.locator.get_model('ExchangeRate')

    def create_exchange_rate(self, params):
        def _rollback(exchange_rate_vo):
            _LOGGER.info(f'[create_exchange_rate._rollback] '
                         f'Delete exchange rate : {exchange_rate_vo.currency}')
            exchange_rate_vo.delete()

        exchange_rate_vo: ExchangeRate = self.exchange_rate_model.create(params)
        self.transaction.add_rollback(_rollback, exchange_rate_vo)

        return exchange_rate_vo

    def update_exchange_rate_by_vo(self, params, exchange_rate_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_exchange_rate_by_vo._rollback] Revert Data : '
                         f'{old_data["currency"]}')
            exchange_rate_vo.update(old_data)

        self.transaction.add_rollback(_rollback, exchange_rate_vo.to_dict())
        return exchange_rate_vo.update(params)

    def delete_exchange_rate(self, currency, domain_id):
        exchange_rate_vo: ExchangeRate = self.get_exchange_rate(currency, domain_id)
        exchange_rate_vo.delete()

    def get_exchange_rate(self, currency, domain_id, only=None):
        return self.exchange_rate_model.get(currency=currency, domain_id=domain_id, only=only)

    def filter_exchange_rates(self, **conditions):
        return self.exchange_rate_model.filter(**conditions)

    def list_exchange_rates(self, query={}):
        return self.exchange_rate_model.query(**query)
