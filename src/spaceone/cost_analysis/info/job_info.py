import functools
from typing import List
from spaceone.api.cost_analysis.v1 import job_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.job_model import Job, Changed, SyncedAccount

__all__ = ["JobInfo", "JobsInfo"]


def ChangedInfo(changed_vos: List[Changed]):
    if changed_vos is None:
        changed_vos = []

    changed_info = []

    for vo in changed_vos:
        info = {
            "start": vo.start,
            "end": vo.end,
            "filter": change_struct_type(vo.filter),
        }

        changed_info.append(job_pb2.ChangedInfo(**info))

    return changed_info


def SyncedAccountInfo(synced_account_vos: List[SyncedAccount]):
    if synced_account_vos is None:
        synced_account_vos = []

    synced_account_info = []

    for vo in synced_account_vos:
        info = {
            "account_id": vo.account_id,
        }

        synced_account_info.append(job_pb2.SyncedAccountInfo(**info))

    return synced_account_info


def JobInfo(job_vo: Job, minimal=False):
    info = {
        "job_id": job_vo.job_id,
        "status": job_vo.status,
        "total_tasks": job_vo.total_tasks,
        "remained_tasks": job_vo.remained_tasks,
        "data_source_id": job_vo.data_source_id,
        "workspace_id": job_vo.workspace_id,
    }

    if not minimal:
        info.update(
            {
                "options": change_struct_type(job_vo.options),
                "error_code": job_vo.error_code,
                "error_message": job_vo.error_message,
                "resource_group": job_vo.resource_group,
                "domain_id": job_vo.domain_id,
                "changed": ChangedInfo(job_vo.changed),
                "synced_accounts": SyncedAccountInfo(job_vo.synced_accounts),
                "created_at": utils.datetime_to_iso8601(job_vo.created_at),
                "updated_at": utils.datetime_to_iso8601(job_vo.updated_at),
                "finished_at": utils.datetime_to_iso8601(job_vo.finished_at),
            }
        )

    return job_pb2.JobInfo(**info)


def JobsInfo(job_vos, total_count, **kwargs):
    return job_pb2.JobsInfo(
        results=list(map(functools.partial(JobInfo, **kwargs), job_vos)),
        total_count=total_count,
    )
