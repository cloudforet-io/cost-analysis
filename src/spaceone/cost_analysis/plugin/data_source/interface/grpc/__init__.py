from spaceone.core.pygrpc.server import GRPCServer
from spaceone.cost_analysis.plugin.data_source.interface.grpc.data_source import (
    DataSource,
)
from spaceone.cost_analysis.plugin.data_source.interface.grpc.job import Job
from spaceone.cost_analysis.plugin.data_source.interface.grpc.cost import Cost

_all_ = ["app"]

app = GRPCServer()
app.add_service(DataSource)
app.add_service(Job)
app.add_service(Cost)
