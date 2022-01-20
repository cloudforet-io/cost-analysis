import functools
from spaceone.api.cost_analysis.v1 import cost_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.cost_model import Cost

__all__ = ['CostInfo', 'CostsInfo']


def CostInfo(cost_vo: Cost, minimal=False):
    info = {
        'cost_id': cost_vo.cost_id,
        'usd_cost': cost_vo.usd_cost,
        'provider': cost_vo.provider,
        'region_code': cost_vo.region_code,
        'category': cost_vo.category,
        'product': cost_vo.product,
        'account': cost_vo.account,
        'usage_type': cost_vo.usage_type,
        'resource_group': cost_vo.resource_group,
        'resource': cost_vo.resource,
        'data_source_id': cost_vo.data_source_id,
        'billed_at': utils.datetime_to_iso8601(cost_vo.billed_at)
    }

    if not minimal:
        info.update({
            'original_currency': cost_vo.original_currency,
            'original_cost': cost_vo.original_cost,
            'usage_quantity': cost_vo.usage_quantity,
            'tags': change_struct_type(cost_vo.tags),
            'additional_info': change_struct_type(cost_vo.additional_info),
            'service_account_id': cost_vo.service_account_id,
            'project_id': cost_vo.project_id,
            'data_source_id': cost_vo.data_source_id,
            'domain_id': cost_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cost_vo.created_at)
        })

    return cost_pb2.CostInfo(**info)


def CostsInfo(cost_vos, total_count, **kwargs):
    return cost_pb2.CostsInfo(results=list(
        map(functools.partial(CostInfo, **kwargs), cost_vos)), total_count=total_count)
