from spaceone.api.cost_analysis.v1 import exchange_rate_pb2, exchange_rate_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class ExchangeRate(BaseAPI, exchange_rate_pb2_grpc.ExchangeRateServicer):

    pb2 = exchange_rate_pb2
    pb2_grpc = exchange_rate_pb2_grpc

    def set(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            return self.locator.get_info('ExchangeRateInfo', exchange_rate_service.set(params))

    def reset(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            return self.locator.get_info('ExchangeRateInfo', exchange_rate_service.reset(params))

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            return self.locator.get_info('ExchangeRateInfo', exchange_rate_service.enable(params))

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            return self.locator.get_info('ExchangeRateInfo', exchange_rate_service.disable(params))

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            return self.locator.get_info('ExchangeRateInfo', exchange_rate_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ExchangeRateService', metadata) as exchange_rate_service:
            exchange_rates_data, total_count = exchange_rate_service.list(params)
            return self.locator.get_info('ExchangeRatesInfo',
                                         exchange_rates_data,
                                         total_count)
