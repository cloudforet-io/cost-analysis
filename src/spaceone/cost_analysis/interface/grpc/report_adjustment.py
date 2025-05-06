from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import (
    report_adjustment_pb2,
    report_adjustment_pb2_grpc,
)

from spaceone.cost_analysis.service.report_adjustment_service import (
    ReportAdjustmentService,
)


class ReportAdjustment(BaseAPI, report_adjustment_pb2_grpc.ReportAdjustmentServicer):
    pb2 = report_adjustment_pb2
    pb2_grpc = report_adjustment_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        response: dict = adjustment_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        response: dict = adjustment_svc.update(params)
        return self.dict_to_message(response)

    def change_order(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        response: dict = adjustment_svc.change_order(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        adjustment_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        response: dict = adjustment_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_svc = ReportAdjustmentService(metadata)
        response: dict = adjustment_svc.list(params)
        return self.dict_to_message(response)
