from spaceone.api.cost_analysis.v1 import budget_pb2, budget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import BudgetService


class Budget(BaseAPI, budget_pb2_grpc.BudgetServicer):

    pb2 = budget_pb2
    pb2_grpc = budget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.update(params)
        return self.dict_to_message(response)

    def set_notification(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.set_notification(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        budget_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        budget_svc = BudgetService(metadata)
        response: dict = budget_svc.stat(params)
        return self.dict_to_message(response)
