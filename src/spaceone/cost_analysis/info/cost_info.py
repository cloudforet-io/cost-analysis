import functools
from spaceone.api.cost_analysis.v1 import cost_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.cost_model import Cost

__all__ = ["CostInfo", "CostsInfo"]


def CostInfo(cost_vo: Cost, minimal=False):
    info = {
        "cost_id": cost_vo.cost_id,
        "cost": cost_vo.cost,
        "provider": cost_vo.provider,
        "region_code": cost_vo.region_code,
        "product": cost_vo.product,
        "usage_type": cost_vo.usage_type,
        "resource": cost_vo.resource,
        "account_id": cost_vo.account_id,
        "data_source_id": cost_vo.data_source_id,
        "workspace_id": cost_vo.workspace_id,
        "billed_date": cost_vo.billed_date,
        "data": change_struct_type(cost_vo.data),
    }

    if not minimal:
        info.update(
            {
                "usage_quantity": cost_vo.usage_quantity,
                "usage_unit": cost_vo.usage_unit,
                "tags": change_struct_type(cost_vo.tags),
                "additional_info": change_struct_type(cost_vo.additional_info),
                "service_account_id": cost_vo.service_account_id,
                "project_id": cost_vo.project_id,
                "data_source_id": cost_vo.data_source_id,
                "domain_id": cost_vo.domain_id,
                "billed_year": cost_vo.billed_year,
                "billed_month": cost_vo.billed_month,
            }
        )

    return cost_pb2.CostInfo(**info)


def CostsInfo(cost_vos, total_count, **kwargs):
    return cost_pb2.CostsInfo(
        results=list(map(functools.partial(CostInfo, **kwargs), cost_vos)),
        total_count=total_count,
    )
