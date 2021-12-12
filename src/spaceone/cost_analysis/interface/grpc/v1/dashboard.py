from spaceone.api.cost_analysis.v1 import dashboard_pb2, dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Dashboard(BaseAPI, dashboard_pb2_grpc.DashboardServicer):

    pb2 = dashboard_pb2
    pb2_grpc = dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            return self.locator.get_info('DashboardInfo', dashboard_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            return self.locator.get_info('DashboardInfo', dashboard_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            dashboard_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            return self.locator.get_info('DashboardInfo', dashboard_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            dashboard_vos, total_count = dashboard_service.list(params)
            return self.locator.get_info('DashboardsInfo',
                                         dashboard_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DashboardService', metadata) as dashboard_service:
            return self.locator.get_info('StatisticsInfo', dashboard_service.stat(params))
