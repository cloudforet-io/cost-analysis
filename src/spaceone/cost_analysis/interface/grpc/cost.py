from spaceone.api.cost_analysis.v1 import cost_pb2, cost_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import CostService


class Cost(BaseAPI, cost_pb2_grpc.CostServicer):
    pb2 = cost_pb2
    pb2_grpc = cost_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service("CostService", metadata) as cost_service:
            return self.locator.get_info("CostInfo", cost_service.create(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service("CostService", metadata) as cost_service:
            cost_service.delete(params)
            return self.locator.get_info("EmptyInfo")

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_svc = CostService(metadata)
        response: dict = cost_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_svc = CostService(metadata)
        response: dict = cost_svc.list(params)
        return self.dict_to_message(response)

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service("CostService", metadata) as cost_service:
            return self.locator.get_info("StatisticsInfo", cost_service.analyze(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service("CostService", metadata) as cost_service:
            return self.locator.get_info("StatisticsInfo", cost_service.stat(params))
