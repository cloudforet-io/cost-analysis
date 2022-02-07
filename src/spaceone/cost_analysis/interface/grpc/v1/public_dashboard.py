from spaceone.api.cost_analysis.v1 import public_dashboard_pb2, public_dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class PublicDashboard(BaseAPI, public_dashboard_pb2_grpc.PublicDashboardServicer):

    pb2 = public_dashboard_pb2
    pb2_grpc = public_dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            return self.locator.get_info('PublicDashboardInfo', public_dashboard_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            return self.locator.get_info('PublicDashboardInfo', public_dashboard_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            public_dashboard_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            return self.locator.get_info('PublicDashboardInfo', public_dashboard_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            public_dashboard_vos, total_count = public_dashboard_service.list(params)
            return self.locator.get_info('PublicDashboardsInfo',
                                         public_dashboard_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PublicDashboardService', metadata) as public_dashboard_service:
            return self.locator.get_info('StatisticsInfo', public_dashboard_service.stat(params))
