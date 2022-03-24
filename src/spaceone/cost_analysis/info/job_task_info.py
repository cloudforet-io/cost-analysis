import functools
from spaceone.api.cost_analysis.v1 import job_task_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.job_task_model import JobTask

__all__ = ['JobTaskInfo', 'JobTasksInfo']


def JobTaskInfo(job_task_vo: JobTask, minimal=False):
    info = {
        'job_task_id': job_task_vo.job_task_id,
        'status': job_task_vo.status,
        'created_count': job_task_vo.created_count,
        'job_id': job_task_vo.job_id,
        'data_source_id': job_task_vo.data_source_id
    }

    if not minimal:
        info.update({
            'options': change_struct_type(job_task_vo.options),
            'error_code': job_task_vo.error_code,
            'error_message': job_task_vo.error_message,
            'job_id': job_task_vo.job_id,
            'domain_id': job_task_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(job_task_vo.created_at),
            'started_at': utils.datetime_to_iso8601(job_task_vo.started_at),
            'updated_at': utils.datetime_to_iso8601(job_task_vo.updated_at),
            'finished_at': utils.datetime_to_iso8601(job_task_vo.finished_at)
        })

    return job_task_pb2.JobTaskInfo(**info)


def JobTasksInfo(job_task_vos, total_count, **kwargs):
    return job_task_pb2.JobTasksInfo(results=list(
        map(functools.partial(JobTaskInfo, **kwargs), job_task_vos)), total_count=total_count)
