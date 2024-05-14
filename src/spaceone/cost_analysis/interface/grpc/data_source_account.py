import logging

from spaceone.api.cost_analysis.v1 import (
    data_source_account_pb2,
    data_source_account_pb2_grpc,
)
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service.data_source_account_service import (
    DataSourceAccountService,
)

_LOGGER = logging.getLogger(__name__)


class DataSourceAccount(
    BaseAPI, data_source_account_pb2_grpc.DataSourceAccountServicer
):
    pb2 = data_source_account_pb2
    pb2_grpc = data_source_account_pb2_grpc

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        response: dict = data_source_account_svc.update(params)
        return self.dict_to_message(response)

    def reset(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        data_source_account_svc.reset(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        response: dict = data_source_account_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        response: dict = data_source_account_svc.list(params)
        return self.dict_to_message(response)

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        response: dict = data_source_account_svc.analyze(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_account_svc = DataSourceAccountService(metadata)
        response: dict = data_source_account_svc.stat(params)
        return self.dict_to_message(response)
