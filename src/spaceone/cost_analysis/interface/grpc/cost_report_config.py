from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import (
    cost_report_config_pb2,
    cost_report_config_pb2_grpc,
)

from spaceone.cost_analysis.service.cost_report_config_service import (
    CostReportConfigService,
)


class CostReportConfig(BaseAPI, cost_report_config_pb2_grpc.CostReportConfigServicer):
    pb2 = cost_report_config_pb2
    pb2_grpc = cost_report_config_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.update(params)
        return self.dict_to_message(response)

    def update_recipients(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.update_recipients(params)
        return self.dict_to_message(response)

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.enable(params)
        return self.dict_to_message(response)

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.disable(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        cost_report_svc.delete(params)
        return self.empty()

    def run(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        cost_report_svc.run(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = CostReportConfigService(metadata)
        response: dict = cost_report_svc.stat(params)
        return self.dict_to_message(response)
