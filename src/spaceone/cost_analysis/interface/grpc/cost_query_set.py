from spaceone.api.cost_analysis.v1 import cost_query_set_pb2, cost_query_set_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CostQuerySet(BaseAPI, cost_query_set_pb2_grpc.CostQuerySetServicer):

    pb2 = cost_query_set_pb2
    pb2_grpc = cost_query_set_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            return self.locator.get_info('CostQuerySetInfo', cost_query_set_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            return self.locator.get_info('CostQuerySetInfo', cost_query_set_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            cost_query_set_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            return self.locator.get_info('CostQuerySetInfo', cost_query_set_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            cost_query_set_vos, total_count = cost_query_set_service.list(params)
            return self.locator.get_info('CostQuerySetsInfo',
                                         cost_query_set_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostQuerySetService', metadata) as cost_query_set_service:
            return self.locator.get_info('StatisticsInfo', cost_query_set_service.stat(params))
