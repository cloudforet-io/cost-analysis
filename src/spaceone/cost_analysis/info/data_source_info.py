import functools
from spaceone.api.cost_analysis.v1 import data_source_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.cost_analysis.model.data_source_model import DataSource

__all__ = ["DataSourceInfo", "DataSourcesInfo"]


def SecretFilterInfo(secret_filter_vo):
    if secret_filter_vo:
        info = {
            "state": secret_filter_vo.state,
            "secrets": secret_filter_vo.secrets,
            "service_accounts": secret_filter_vo.service_accounts,
            "schemas": secret_filter_vo.schemas,
        }
        return data_source_pb2.SecretFilter(**info)
    else:
        return None


def PluginInfo(vo):
    if vo:
        info = {
            "plugin_id": vo.plugin_id,
            "version": vo.version,
            "options": change_struct_type(vo.options),
            "metadata": change_struct_type(vo.metadata),
            "secret_id": vo.secret_id,
            "schema_id": vo.schema_id,
            "upgrade_mode": vo.upgrade_mode,
        }

        return data_source_pb2.PluginInfo(**info)
    else:
        return None


def DataSourceInfo(data_source_vo: DataSource, minimal=False):
    info = {
        "data_source_id": data_source_vo.data_source_id,
        "workspace_id": data_source_vo.workspace_id,
        "name": data_source_vo.name,
        "state": data_source_vo.state,
        "data_source_type": data_source_vo.data_source_type,
        "secret_type": data_source_vo.secret_type,
        "provider": data_source_vo.provider,
        "resource_group": data_source_vo.resource_group,
    }

    if not minimal:
        info.update(
            {
                "plugin_info": PluginInfo(data_source_vo.plugin_info),
                "secret_filter": SecretFilterInfo(data_source_vo.secret_filter),
                "template": change_struct_type(data_source_vo.template),
                "permissions": change_struct_type(data_source_vo.permissions),
                "tags": change_struct_type(data_source_vo.tags),
                "cost_tag_keys": data_source_vo.cost_tag_keys,
                "cost_additional_info_keys": data_source_vo.cost_additional_info_keys,
                "cost_data_keys": data_source_vo.cost_data_keys,
                "data_source_account_count": data_source_vo.data_source_account_count,
                "connected_workspace_count": data_source_vo.connected_workspace_count,
                "domain_id": data_source_vo.domain_id,
                "created_at": utils.datetime_to_iso8601(data_source_vo.created_at),
                "last_synchronized_at": utils.datetime_to_iso8601(
                    data_source_vo.last_synchronized_at
                ),
            }
        )

    return data_source_pb2.DataSourceInfo(**info)


def DataSourcesInfo(data_source_vos, total_count, **kwargs):
    return data_source_pb2.DataSourcesInfo(
        results=list(map(functools.partial(DataSourceInfo, **kwargs), data_source_vos)),
        total_count=total_count,
    )
