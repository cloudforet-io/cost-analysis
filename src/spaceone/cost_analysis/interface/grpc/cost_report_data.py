from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import (
    cost_report_data_pb2,
    cost_report_data_pb2_grpc,
)

from spaceone.cost_analysis.service.cost_report_data_service import (
    CostReportDataService,
)


class CostReportData(BaseAPI, cost_report_data_pb2_grpc.CostReportDataServicer):
    pb2 = cost_report_data_pb2
    pb2_grpc = cost_report_data_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_data_svc = CostReportDataService(metadata)
        response: dict = cost_report_data_svc.list(params)
        return self.dict_to_message(response)

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_data_svc = CostReportDataService(metadata)
        response: dict = cost_report_data_svc.analyze(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_data_svc = CostReportDataService(metadata)
        response: dict = cost_report_data_svc.stat(params)
        return self.dict_to_message(response)
