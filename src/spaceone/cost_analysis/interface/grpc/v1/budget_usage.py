from spaceone.api.cost_analysis.v1 import budget_usage_pb2, budget_usage_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class BudgetUsage(BaseAPI, budget_usage_pb2_grpc.BudgetUsageServicer):

    pb2 = budget_usage_pb2
    pb2_grpc = budget_usage_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetUsageService', metadata) as budget_usage_service:
            budget_usage_vos, total_count = budget_usage_service.list(params)
            return self.locator.get_info('BudgetUsagesInfo',
                                         budget_usage_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('BudgetUsageService', metadata) as budget_usage_service:
            return self.locator.get_info('StatisticsInfo', budget_usage_service.stat(params))
