from spaceone.api.cost_analysis.v1 import budget_pb2, budget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Budget(BaseAPI, budget_pb2_grpc.BudgetServicer):

    pb2 = budget_pb2
    pb2_grpc = budget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            return self.locator.get_info('BudgetInfo', budget_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            return self.locator.get_info('BudgetInfo', budget_service.update(params))

    def set_notification(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            return self.locator.get_info('BudgetInfo', budget_service.set_notification(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            budget_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            return self.locator.get_info('BudgetInfo', budget_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            budget_vos, total_count = budget_service.list(params)
            return self.locator.get_info('BudgetsInfo',
                                         budget_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetService', metadata) as budget_service:
            return self.locator.get_info('StatisticsInfo', budget_service.stat(params))
