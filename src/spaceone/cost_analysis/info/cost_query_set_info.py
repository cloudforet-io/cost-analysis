import functools
from spaceone.api.cost_analysis.v1 import cost_query_set_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.cost_query_set_model import CostQuerySet

__all__ = ['CostQuerySetInfo', 'CostQuerySetsInfo']


def CostQuerySetInfo(cost_query_set_vo: CostQuerySet, minimal=False):
    info = {
        'cost_query_set_id': cost_query_set_vo.cost_query_set_id,
        'name': cost_query_set_vo.name,
        'user_id': cost_query_set_vo.user_id,
    }

    if not minimal:
        info.update({
            'options': change_struct_type(cost_query_set_vo.options),
            'tags': change_struct_type(cost_query_set_vo.tags),
            'domain_id': cost_query_set_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cost_query_set_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(cost_query_set_vo.updated_at),
        })

    return cost_query_set_pb2.CostQuerySetInfo(**info)


def CostQuerySetsInfo(cost_query_set_vos, total_count, **kwargs):
    return cost_query_set_pb2.CostQuerySetsInfo(results=list(
        map(functools.partial(CostQuerySetInfo, **kwargs), cost_query_set_vos)), total_count=total_count)
