import functools
from spaceone.api.cost_analysis.v1 import user_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.user_dashboard_model import UserDashboard

__all__ = ['UserDashboardInfo', 'UserDashboardsInfo']


def UserDashboardPeriodInfo(vo):
    if vo:
        info = {
            'start': vo.start,
            'end': vo.end,
        }

        return user_dashboard_pb2.UserDashboardPeriod(**info)
    else:
        return None


def UserDashboardInfo(user_dashboard_vo: UserDashboard, minimal=False):
    info = {
        'user_dashboard_id': user_dashboard_vo.user_dashboard_id,
        'name': user_dashboard_vo.name,
        'period_type': user_dashboard_vo.period_type,
        'user_id': user_dashboard_vo.user_id,
    }

    if not minimal:
        info.update({
            'default_layout_id': user_dashboard_vo.default_layout_id,
            'custom_layouts': change_list_value_type(user_dashboard_vo.custom_layouts) if user_dashboard_vo.custom_layouts else None,
            'default_filter': change_struct_type(user_dashboard_vo.default_filter),
            'period': UserDashboardPeriodInfo(user_dashboard_vo.period),
            'tags': change_struct_type(user_dashboard_vo.tags),
            'domain_id': user_dashboard_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(user_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(user_dashboard_vo.updated_at),
        })

    return user_dashboard_pb2.UserDashboardInfo(**info)


def UserDashboardsInfo(user_dashboard_vos, total_count, **kwargs):
    return user_dashboard_pb2.UserDashboardsInfo(results=list(
        map(functools.partial(UserDashboardInfo, **kwargs), user_dashboard_vos)), total_count=total_count)
