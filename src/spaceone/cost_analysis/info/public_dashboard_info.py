import functools
from spaceone.api.cost_analysis.v1 import public_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.public_dashboard_model import PublicDashboard

__all__ = ['PublicDashboardInfo', 'PublicDashboardsInfo']


def PublicDashboardPeriodInfo(vo):
    if vo:
        info = {
            'start': vo.start,
            'end': vo.end,
        }

        return public_dashboard_pb2.PublicDashboardPeriod(**info)
    else:
        return None


def PublicDashboardInfo(public_dashboard_vo: PublicDashboard, minimal=False):
    info = {
        'public_dashboard_id': public_dashboard_vo.public_dashboard_id,
        'name': public_dashboard_vo.name,
        'period_type': public_dashboard_vo.period_type,
    }

    if not minimal:
        info.update({
            'default_layout_id': public_dashboard_vo.default_layout_id,
            'custom_layouts': change_list_value_type(public_dashboard_vo.custom_layouts) if public_dashboard_vo.custom_layouts else None,
            'default_filter': change_struct_type(public_dashboard_vo.default_filter),
            'period': PublicDashboardPeriodInfo(public_dashboard_vo.period),
            'tags': change_struct_type(public_dashboard_vo.tags),
            'domain_id': public_dashboard_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(public_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(public_dashboard_vo.updated_at),
        })

    return public_dashboard_pb2.PublicDashboardInfo(**info)


def PublicDashboardsInfo(public_dashboard_vos, total_count, **kwargs):
    return public_dashboard_pb2.PublicDashboardsInfo(results=list(
        map(functools.partial(PublicDashboardInfo, **kwargs), public_dashboard_vos)), total_count=total_count)
