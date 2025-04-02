from http.client import responses

from spaceone.api.cost_analysis.v1 import job_task_pb2, job_task_pb2_grpc
from spaceone.core.pygrpc import BaseAPI

from spaceone.cost_analysis.service import JobTaskService


class JobTask(BaseAPI, job_task_pb2_grpc.JobTaskServicer):
    pb2 = job_task_pb2
    pb2_grpc = job_task_pb2_grpc

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        job_task_svc = JobTaskService(metadata)
        response: dict = job_task_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        job_task_svc = JobTaskService(metadata)
        response: dict = job_task_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        job_task_svc = JobTaskService(metadata)
        response: dict = job_task_svc.stat(params)
        return self.dict_to_message(response)
