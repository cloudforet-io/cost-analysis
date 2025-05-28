from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.v1 import (
    report_adjustment_policy_pb2,
    report_adjustment_policy_pb2_grpc,
)

from spaceone.cost_analysis.service.report_adjustment_policy_service import (
    ReportAdjustmentPolicyService,
)


class ReportAdjustmentPolicy(
    BaseAPI, report_adjustment_policy_pb2_grpc.ReportAdjustmentPolicyServicer
):
    pb2 = report_adjustment_policy_pb2
    pb2_grpc = report_adjustment_policy_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.update(params)
        return self.dict_to_message(response)

    def change_order(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.change_order(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        cost_report_svc = ReportAdjustmentPolicyService(metadata)
        cost_report_svc.delete(params)
        return self.empty()

    def sync_currency(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.sync_currency(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        adjustment_policy_svc = ReportAdjustmentPolicyService(metadata)
        response: dict = adjustment_policy_svc.list(params)
        return self.dict_to_message(response)
