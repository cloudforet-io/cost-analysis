import functools
from spaceone.api.cost_analysis.v1 import job_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.job_model import Job

__all__ = ['JobInfo', 'JobsInfo']


def JobInfo(job_vo: Job, minimal=False):
    info = {
        'job_id': job_vo.job_id,
        'status': job_vo.status,
        'total_tasks': job_vo.total_tasks,
        'remained_tasks': job_vo.remained_tasks,
        'data_source_id': job_vo.data_source_id
    }

    if not minimal:
        info.update({
            'error_code': job_vo.error_code,
            'error_message': job_vo.error_message,
            'domain_id': job_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(job_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(job_vo.updated_at),
            'finished_at': utils.datetime_to_iso8601(job_vo.finished_at),
            'last_changed_at': utils.datetime_to_iso8601(job_vo.last_changed_at)
        })

    return job_pb2.JobInfo(**info)


def JobsInfo(job_vos, total_count, **kwargs):
    return job_pb2.JobsInfo(results=list(
        map(functools.partial(JobInfo, **kwargs), job_vos)), total_count=total_count)
