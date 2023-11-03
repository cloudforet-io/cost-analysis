from spaceone.core.pygrpc import BaseAPI
from spaceone.api.cost_analysis.plugin import data_source_pb2, data_source_pb2_grpc
from spaceone.cost_analysis.plugin.data_source.service.data_source_service import DataSourceService


class DataSource(BaseAPI, data_source_pb2_grpc.DataSourceServicer):

    pb2 = data_source_pb2
    pb2_grpc = data_source_pb2_grpc

    def init(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.init(params)
        return self.dict_to_message(response)

    def verify(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        data_source_svc.verify(params)
        return self.empty()
