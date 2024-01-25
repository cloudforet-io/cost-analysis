from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import cost_report_pb2, cost_report_pb2_grpc

from spaceone.cost_analysis.service.cost_report_service import CostReportService


class CostReport(BaseAPI, cost_report_pb2, cost_report_pb2_grpc):
    pb2 = cost_report_pb2
    pb2_grpc = cost_report_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportService(metadata)
        response: dict = cost_report_svc.create(params)
        return self.dict_to_message(response)
