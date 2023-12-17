import functools
from spaceone.api.cost_analysis.v1 import budget_usage_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.budget_usage_model import BudgetUsage, ProviderFilter

__all__ = ["BudgetUsageInfo", "BudgetUsagesInfo"]


def ProviderFilterInfo(provider_filter_vo: ProviderFilter):
    if provider_filter_vo is None:
        return None

    info = {
        "state": provider_filter_vo.state,
        "providers": list(provider_filter_vo.providers),
    }

    return budget_usage_pb2.BudgetUsageProviderFilter(**info)


def BudgetUsageInfo(budget_usage_vo: BudgetUsage, minimal=False):
    info = {
        "budget_id": budget_usage_vo.budget_id,
        "name": budget_usage_vo.name,
        "date": budget_usage_vo.date,
        "cost": budget_usage_vo.cost,
        "limit": budget_usage_vo.limit,
        "currency": budget_usage_vo.currency,
        "provider_filter": ProviderFilterInfo(budget_usage_vo.provider_filter),
        "resource_group": budget_usage_vo.resource_group,
        "project_id": budget_usage_vo.project_id,
        "data_source_id": budget_usage_vo.data_source_id,
        "workspace_id": budget_usage_vo.workspace_id,
    }

    if not minimal:
        info.update(
            {
                "domain_id": budget_usage_vo.domain_id,
                "updated_at": utils.datetime_to_iso8601(budget_usage_vo.updated_at),
            }
        )

    return budget_usage_pb2.BudgetUsageInfo(**info)


def BudgetUsagesInfo(budget_usage_vos, total_count, **kwargs):
    return budget_usage_pb2.BudgetUsagesInfo(
        results=list(
            map(functools.partial(BudgetUsageInfo, **kwargs), budget_usage_vos)
        ),
        total_count=total_count,
    )
