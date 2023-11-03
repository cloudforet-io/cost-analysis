from spaceone.core.pygrpc.server import GRPCServer
from .budget import Budget
from .budget_usage import BudgetUsage
from .cost import Cost
from .cost_query_set import CostQuerySet
from .data_source import DataSource
from .data_source_rule import DataSourceRule
from .job import Job
from .job_task import JobTask


_all_ = ['app']

app = GRPCServer()
app.add_service(Budget)
app.add_service(BudgetUsage)
app.add_service(Cost)
app.add_service(CostQuerySet)
app.add_service(DataSource)
app.add_service(DataSourceRule)
app.add_service(Job)
app.add_service(JobTask)
