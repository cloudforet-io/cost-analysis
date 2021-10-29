import functools
from spaceone.api.cost_analysis.v1 import schedule_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.schedule_model import Schedule, Scheduled

__all__ = ['ScheduleInfo', 'SchedulesInfo']


def ScheduledInfo(vo: Scheduled):
    info = {
        'cron': vo.cron,
        'interval': vo.interval,
        'hours': vo.hours
    }
    return schedule_pb2.Scheduled(**info)


def ScheduleInfo(schedule_vo: Schedule, minimal=False):
    info = {
        'schedule_id': schedule_vo.schedule_id,
        'name': schedule_vo.name,
        'state': schedule_vo.state,
    }

    if not minimal:
        info.update({
            'schedule': ScheduledInfo(schedule_vo.schedule) if schedule_vo.schedule else None,
            'options': change_struct_type(schedule_vo.options) if schedule_vo.options else None,
            'tags': change_struct_type(schedule_vo.tags),
            'data_source_id': schedule_vo.data_source_id,
            'domain_id': schedule_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(schedule_vo.created_at),
            'last_scheduled_at': utils.datetime_to_iso8601(schedule_vo.last_scheduled_at)
        })

    return schedule_pb2.ScheduleInfo(**info)


def SchedulesInfo(schedule_vos, total_count, **kwargs):
    return schedule_pb2.SchedulesInfo(results=list(
        map(functools.partial(ScheduleInfo, **kwargs), schedule_vos)), total_count=total_count)
