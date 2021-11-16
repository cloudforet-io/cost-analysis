import logging

from spaceone.core.service import *
from spaceone.core import utils, config
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.exchange_rate_manager import ExchangeRateManager
from spaceone.cost_analysis.model.exchange_rate_model import ExchangeRate

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ExchangeRateService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exchange_rate_mgr: ExchangeRateManager = self.locator.get_manager('ExchangeRateManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['currency', 'rate', 'domain_id'])
    def set(self, params):
        """Set exchange rate

        Args:
            params (dict): {
                'currency': 'str',
                'rate': 'float',
                'domain_id': 'str'
            }

        Returns:
            exchange_rate_data (dict)
        """

        domain_id = params['domain_id']
        currency = params['currency']
        rate = params['rate']

        default_exchange_rates = config.get_global('DEFAULT_EXCHANGE_RATE', {})

        if currency not in default_exchange_rates:
            raise ERROR_UNSUPPORTED_CURRENCY(supported_currency=default_exchange_rates.keys())

        try:
            exchange_rate_vo: ExchangeRate = self.exchange_rate_mgr.get_exchange_rate(currency, domain_id)
            updated_exchange_rate_vo: ExchangeRate = self.exchange_rate_mgr.update_exchange_rate_by_vo({'rate': rate},
                                                                                                       exchange_rate_vo)
            return updated_exchange_rate_vo.to_dict()
        except Exception as e:
            exchange_rate_vo: ExchangeRate = self.exchange_rate_mgr.create_exchange_rate(params)
            return exchange_rate_vo.to_dict()

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['currency', 'domain_id'])
    def reset(self, params):
        """Reset exchange rate

        Args:
            params (dict): {
                'currency': 'str',
                'domain_id': 'str'
            }

        Returns:
            exchange_rate_data (dict)
        """

        domain_id = params['domain_id']
        currency = params['currency']

        default_exchange_rates = config.get_global('DEFAULT_EXCHANGE_RATE', {})

        if currency not in default_exchange_rates:
            raise ERROR_UNSUPPORTED_CURRENCY(supported_currency=default_exchange_rates.keys())

        try:
            exchange_rate_vo: ExchangeRate = self.exchange_rate_mgr.get_exchange_rate(currency, domain_id)
            exchange_rate_vo.delete()
        except Exception as e:
            pass

        return {
            'currency': currency,
            'rate': default_exchange_rates[currency],
            'domain_id': domain_id,
            'is_default': True
        }

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['currency', 'domain_id'])
    def get(self, params):
        """ Get exchange rate

        Args:
            params (dict): {
                'currency': 'str',
                'domain_id': 'str'
            }

        Returns:
            exchange_rate_data (dict)
        """

        domain_id = params['domain_id']
        currency = params['currency']

        default_exchange_rates = config.get_global('DEFAULT_EXCHANGE_RATE', {})

        if currency not in default_exchange_rates:
            raise ERROR_UNSUPPORTED_CURRENCY(supported_currency=default_exchange_rates.keys())

        try:
            exchange_rate_vo: ExchangeRate = self.exchange_rate_mgr.get_exchange_rate(currency, domain_id)
            return exchange_rate_vo.to_dict()
        except Exception as e:
            pass

        return {
            'currency': currency,
            'rate': default_exchange_rates[currency],
            'domain_id': domain_id,
            'is_default': True
        }

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    def list(self, params):
        """ List exchange rates

        Args:
            params (dict): {
                'domain_id': 'str'
            }

        Returns:
            exchange_rates_data (dict)
            total_count (int)
        """

        domain_id = params['domain_id']

        results = []
        custom_exchange_rates = []

        exchange_rate_vos = self.exchange_rate_mgr.filter_exchange_rates(domain_id=domain_id)
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
