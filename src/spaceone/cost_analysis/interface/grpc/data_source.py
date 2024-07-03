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

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "DataSourceInfo", data_source_service.update(params)
            )

    def update_permissions(self, request, context):
        params, metadata = self.parse_request(request, context)
        data_source_svc = DataSourceService(metadata)
        response: dict = data_source_svc.update_permissions(params)
        return self.dict_to_message(response)

    def update_secret_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "DataSourceInfo", data_source_service.update_secret_data(params)
            )

    def update_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "DataSourceInfo", data_source_service.update_plugin(params)
            )

    def verify_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            data_source_service.verify_plugin(params)
            return self.locator.get_info("EmptyInfo")

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "DataSourceInfo", data_source_service.enable(params)
            )

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "DataSourceInfo", data_source_service.disable(params)
            )

    def deregister(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            data_source_service.deregister(params)
            return self.locator.get_info("EmptyInfo")

    def sync(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info("JobInfo", data_source_service.sync(params))

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

        with self.locator.get_service(
            "DataSourceService", metadata
        ) as data_source_service:
            return self.locator.get_info(
                "StatisticsInfo", data_source_service.stat(params)
            )
