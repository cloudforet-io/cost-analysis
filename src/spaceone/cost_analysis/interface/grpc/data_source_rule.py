from spaceone.api.cost_analysis.v1 import data_source_rule_pb2, data_source_rule_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import DataSourceRuleService


class DataSourceRule(BaseAPI, data_source_rule_pb2_grpc.DataSourceRuleServicer):

    pb2 = data_source_rule_pb2
    pb2_grpc = data_source_rule_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.update(params)
        return self.dict_to_message(response)

    def change_order(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.change_order(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        data_source_rule_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_rule_svc = DataSourceRuleService(metadata)
        response: dict = data_source_rule_svc.stat(params)
        return self.dict_to_message(response)
