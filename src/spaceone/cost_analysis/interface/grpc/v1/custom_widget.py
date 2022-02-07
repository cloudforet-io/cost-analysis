from spaceone.api.cost_analysis.v1 import custom_widget_pb2, custom_widget_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CustomWidget(BaseAPI, custom_widget_pb2_grpc.CustomWidgetServicer):

    pb2 = custom_widget_pb2
    pb2_grpc = custom_widget_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            return self.locator.get_info('CustomWidgetInfo', custom_widget_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            return self.locator.get_info('CustomWidgetInfo', custom_widget_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            custom_widget_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            return self.locator.get_info('CustomWidgetInfo', custom_widget_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            custom_widget_vos, total_count = custom_widget_service.list(params)
            return self.locator.get_info('CustomWidgetsInfo',
                                         custom_widget_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CustomWidgetService', metadata) as custom_widget_service:
            return self.locator.get_info('StatisticsInfo', custom_widget_service.stat(params))
