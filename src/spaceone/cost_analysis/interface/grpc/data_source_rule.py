from spaceone.api.cost_analysis.v1 import data_source_rule_pb2, data_source_rule_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class DataSourceRule(BaseAPI, data_source_rule_pb2_grpc.DataSourceRuleServicer):

    pb2 = data_source_rule_pb2
    pb2_grpc = data_source_rule_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            return self.locator.get_info('DataSourceRuleInfo', data_source_rule_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            return self.locator.get_info('DataSourceRuleInfo', data_source_rule_service.update(params))

    def change_order(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            return self.locator.get_info('DataSourceRuleInfo', data_source_rule_service.change_order(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            data_source_rule_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            return self.locator.get_info('DataSourceRuleInfo', data_source_rule_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            data_source_rule_vos, total_count = data_source_rule_service.list(params)
            return self.locator.get_info('DataSourceRulesInfo',
                                         data_source_rule_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceRuleService', metadata) as data_source_rule_service:
            return self.locator.get_info('StatisticsInfo', data_source_rule_service.stat(params))
