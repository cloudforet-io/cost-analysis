import logging

from spaceone.core import config
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

    def list_all_exchange_rates(self, domain_id):
        results = []
        custom_exchange_rates = []

        exchange_rate_vos = self.filter_exchange_rates(domain_id=domain_id)
        for exchange_rate_vo in exchange_rate_vos:
            results.append(exchange_rate_vo.to_dict())
            custom_exchange_rates.append(exchange_rate_vo.currency)

        default_exchange_rates = config.get_global('DEFAULT_EXCHANGE_RATE', {})

        for currency, rate in default_exchange_rates.items():
            if currency not in custom_exchange_rates:
                results.append({
                    'currency': currency,
                    'rate': rate,
                    'domain_id': domain_id,
                    'is_default': True
                })

        return results, len(results)
