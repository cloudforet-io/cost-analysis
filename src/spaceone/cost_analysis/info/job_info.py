import functools
from typing import List
from spaceone.api.cost_analysis.v1 import job_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.job_model import Job, Changed

__all__ = ['JobInfo', 'JobsInfo']


def ChangedInfo(changed_vos: List[Changed]):
    if changed_vos is None:
        changed_vos = []

    changed_info = []

    for vo in changed_vos:
        info = {
            'start': utils.datetime_to_iso8601(vo.start),
            'end': utils.datetime_to_iso8601(vo.end),
            'filter': change_struct_type(vo.filter)
        }

        changed_info.append(job_pb2.ChangedInfo(**info))

    return changed_info


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
            'options': change_struct_type(job_vo.options),
            'error_code': job_vo.error_code,
            'error_message': job_vo.error_message,
            'domain_id': job_vo.domain_id,
            'changed': ChangedInfo(job_vo.changed),
            'created_at': utils.datetime_to_iso8601(job_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(job_vo.updated_at),
            'finished_at': utils.datetime_to_iso8601(job_vo.finished_at)
        })

    return job_pb2.JobInfo(**info)


def JobsInfo(job_vos, total_count, **kwargs):
    return job_pb2.JobsInfo(results=list(
        map(functools.partial(JobInfo, **kwargs), job_vos)), total_count=total_count)
