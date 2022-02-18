import functools
from spaceone.api.cost_analysis.v1 import custom_widget_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.custom_widget_model import CustomWidget

__all__ = ['CustomWidgetInfo', 'CustomWidgetsInfo']


def CustomWidgetInfo(custom_widget_vo: CustomWidget, minimal=False):
    info = {
        'widget_id': custom_widget_vo.widget_id,
        'name': custom_widget_vo.name,
        'user_id': custom_widget_vo.user_id,
    }

    if not minimal:
        info.update({
            'options': change_struct_type(custom_widget_vo.options),
            'tags': change_struct_type(custom_widget_vo.tags),
            'domain_id': custom_widget_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(custom_widget_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(custom_widget_vo.updated_at),
        })

    return custom_widget_pb2.CustomWidgetInfo(**info)


def CustomWidgetsInfo(custom_widget_vos, total_count, **kwargs):
    return custom_widget_pb2.CustomWidgetsInfo(results=list(
        map(functools.partial(CustomWidgetInfo, **kwargs), custom_widget_vos)), total_count=total_count)
