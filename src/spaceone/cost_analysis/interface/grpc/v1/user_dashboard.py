from spaceone.api.cost_analysis.v1 import user_dashboard_pb2, user_dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class UserDashboard(BaseAPI, user_dashboard_pb2_grpc.UserDashboardServicer):

    pb2 = user_dashboard_pb2
    pb2_grpc = user_dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            return self.locator.get_info('UserDashboardInfo', user_dashboard_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            return self.locator.get_info('UserDashboardInfo', user_dashboard_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            user_dashboard_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            return self.locator.get_info('UserDashboardInfo', user_dashboard_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            user_dashboard_vos, total_count = user_dashboard_service.list(params)
            return self.locator.get_info('UserDashboardsInfo',
                                         user_dashboard_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('UserDashboardService', metadata) as user_dashboard_service:
            return self.locator.get_info('StatisticsInfo', user_dashboard_service.stat(params))
