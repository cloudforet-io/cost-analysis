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
                         f'Delete exchange_rate : {exchange_rate_vo.name} '
                         f'({exchange_rate_vo.exchange_rate_id})')
            exchange_rate_vo.delete()

        exchange_rate_vo: ExchangeRate = self.exchange_rate_model.create(params)
        self.transaction.add_rollback(_rollback, exchange_rate_vo)

        return exchange_rate_vo

    def delete_exchange_rate(self, exchange_rate_id, domain_id):
        exchange_rate_vo: ExchangeRate = self.get_exchange_rate(exchange_rate_id, domain_id)
        exchange_rate_vo.delete()

    def get_exchange_rate(self, exchange_rate_id, domain_id, only=None):
        return self.exchange_rate_model.get(exchange_rate_id=exchange_rate_id, domain_id=domain_id, only=only)

    def list_exchange_rates(self, query={}):
        return self.exchange_rate_model.query(**query)
