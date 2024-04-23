from spaceone.core.pygrpc.server import GRPCServer
from spaceone.core.plugin.server import PluginServer
from spaceone.cost_analysis.plugin.data_source.interface.grpc import app
from spaceone.cost_analysis.plugin.data_source.service.data_source_service import (
    DataSourceService,
)
from spaceone.cost_analysis.plugin.data_source.service.job_service import JobService
from spaceone.cost_analysis.plugin.data_source.service.cost_service import CostService

__all__ = ["DataSourcePluginServer"]


class DataSourcePluginServer(PluginServer):
    _grpc_app: GRPCServer = app
    _global_conf_path: str = (
        "spaceone.cost_analysis.plugin.data_source.conf.global_conf:global_conf"
    )
    _plugin_methods = {
        "DataSource": {"service": DataSourceService, "methods": ["init", "verify"]},
        "Job": {"service": JobService, "methods": ["get_tasks"]},
        "Cost": {
            "service": CostService,
            "methods": ["get_data", "get_linked_accounts"],
        },
    }
