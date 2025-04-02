from spaceone.api.cost_analysis.v1 import cost_query_set_pb2, cost_query_set_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import CostQuerySetService


class CostQuerySet(BaseAPI, cost_query_set_pb2_grpc.CostQuerySetServicer):

    pb2 = cost_query_set_pb2
    pb2_grpc = cost_query_set_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        response: dict = cost_query_set_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        response: dict = cost_query_set_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        cost_query_set_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        response: dict = cost_query_set_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        response: dict = cost_query_set_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_query_set_svc = CostQuerySetService(metadata)
        response: dict = cost_query_set_svc.stat(params)
        return self.dict_to_message(response)
