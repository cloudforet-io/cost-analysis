import logging

from spaceone.api.cost_analysis.v1 import data_source_pb2, data_source_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import DataSourceService

_LOGGER = logging.getLogger(__name__)


class DataSource(BaseAPI, data_source_pb2_grpc.DataSourceServicer):
    pb2 = data_source_pb2
    pb2_grpc = data_source_pb2_grpc

    def register(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.register(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.update(params)
        return self.dict_to_message(response)

    def update_permissions(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.update_permissions(params)
        return self.dict_to_message(response)

    def update_secret_data(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.update_secret_data(params)
        return self.dict_to_message(response)

    def update_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.update_plugin(params)
        return self.dict_to_message(response)

    def verify_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        data_source_svc.verify_plugin(params)
        return self.empty()

    def deregister(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        data_source_svc.deregister(params)
        return self.empty()

    def sync(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.sync(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.stat(params)
        return self.dict_to_message(response)
