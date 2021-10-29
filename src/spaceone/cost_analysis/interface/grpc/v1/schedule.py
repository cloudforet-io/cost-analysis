from spaceone.api.cost_analysis.v1 import schedule_pb2, schedule_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Schedule(BaseAPI, schedule_pb2_grpc.ScheduleServicer):

    pb2 = schedule_pb2
    pb2_grpc = schedule_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('ScheduleInfo', schedule_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('ScheduleInfo', schedule_service.update(params))

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('ScheduleInfo', schedule_service.enable(params))

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('ScheduleInfo', schedule_service.disable(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            schedule_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('ScheduleInfo', schedule_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            schedule_vos, total_count = schedule_service.list(params)
            return self.locator.get_info('SchedulesInfo',
                                         schedule_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ScheduleService', metadata) as schedule_service:
            return self.locator.get_info('StatisticsInfo', schedule_service.stat(params))
