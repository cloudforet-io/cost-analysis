from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import unified_cost_pb2, unified_cost_pb2_grpc

from spaceone.cost_analysis.service.unified_cost_service import UnifiedCostService


class UnifiedCost(BaseAPI, unified_cost_pb2_grpc.UnifiedCostServicer):
    pb2 = unified_cost_pb2
    pb2_grpc = unified_cost_pb2_grpc

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        unified_cost_svc = UnifiedCostService(metadata)
        response: dict = unified_cost_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        unified_cost_svc = UnifiedCostService(metadata)
        response: dict = unified_cost_svc.list(params)
        return self.dict_to_message(response)

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)
        unified_cost_svc = UnifiedCostService(metadata)
        response: dict = unified_cost_svc.analyze(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        unified_cost_svc = UnifiedCostService(metadata)
        response: dict = unified_cost_svc.stat(params)
        return self.dict_to_message(response)
