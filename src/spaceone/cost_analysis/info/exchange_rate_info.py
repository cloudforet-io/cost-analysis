import functools
from spaceone.api.cost_analysis.v1 import exchange_rate_pb2

__all__ = ['ExchangeRateInfo', 'ExchangeRatesInfo']


def ExchangeRateInfo(exchange_rate_data):
    info = {
        'currency': exchange_rate_data['currency'],
        'rate': exchange_rate_data['rate'],
        'state': exchange_rate_data.get('state', 'ENABLED'),
        'is_default': exchange_rate_data.get('is_default', False),
        'domain_id': exchange_rate_data['domain_id']
    }

    return exchange_rate_pb2.ExchangeRateInfo(**info)


def ExchangeRatesInfo(exchange_rates_data, total_count, **kwargs):
    return exchange_rate_pb2.ExchangeRatesInfo(results=list(
        map(functools.partial(ExchangeRateInfo, **kwargs), exchange_rates_data)), total_count=total_count)
