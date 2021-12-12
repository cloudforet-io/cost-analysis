import functools
from spaceone.api.cost_analysis.v1 import dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.dashboard_model import Dashboard

__all__ = ['DashboardInfo', 'DashboardsInfo']


def DashboardPeriodInfo(vo):
    if vo:
        info = {
            'start': vo.start,
            'end': vo.end,
        }

        return dashboard_pb2.DashboardPeriod(**info)
    else:
        return None


def DashboardInfo(dashboard_vo: Dashboard, minimal=False):
    info = {
        'dashboard_id': dashboard_vo.dashboard_id,
        'name': dashboard_vo.name,
        'scope': dashboard_vo.scope,
        'period_type': dashboard_vo.period_type,
        'user_id': dashboard_vo.user_id,
    }

    if not minimal:
        info.update({
            'default_layout_id': dashboard_vo.default_layout_id,
            'custom_layouts': change_list_value_type(dashboard_vo.custom_layouts) if dashboard_vo.custom_layouts else None,
            'default_filter': change_struct_type(dashboard_vo.default_filter),
            'period': DashboardPeriodInfo(dashboard_vo.period),
            'tags': change_struct_type(dashboard_vo.tags),
            'user_id': dashboard_vo.user_id,
            'domain_id': dashboard_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(dashboard_vo.updated_at),
        })

    return dashboard_pb2.DashboardInfo(**info)


def DashboardsInfo(dashboard_vos, total_count, **kwargs):
    return dashboard_pb2.DashboardsInfo(results=list(
        map(functools.partial(DashboardInfo, **kwargs), dashboard_vos)), total_count=total_count)
