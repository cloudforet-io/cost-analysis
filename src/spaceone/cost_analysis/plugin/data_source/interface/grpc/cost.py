from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.plugin import cost_pb2, cost_pb2_grpc
from spaceone.cost_analysis.plugin.data_source.service.cost_service import CostService


class Cost(BaseAPI, cost_pb2_grpc.CostServicer):
    pb2 = cost_pb2
    pb2_grpc = cost_pb2_grpc

    def get_linked_accounts(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_svc = CostService(metadata)
        response: dict = cost_svc.get_linked_accounts(params)
        return self.dict_to_message(response)

    def get_data(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_svc = CostService(metadata)
        for response in cost_svc.get_data(params):
            yield self.dict_to_message(response)
