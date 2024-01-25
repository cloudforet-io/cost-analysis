from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import cost_report_pb2, cost_report_pb2_grpc

from spaceone.cost_analysis.service.cost_report_serivce import CostReportService


class CostReport(BaseAPI, cost_report_pb2_grpc.CostReportServicer):
    pb2 = cost_report_pb2
    pb2_grpc = cost_report_pb2_grpc

    def send(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportService(metadata)
        cost_report_svc.send(params)
        return self.empty()
