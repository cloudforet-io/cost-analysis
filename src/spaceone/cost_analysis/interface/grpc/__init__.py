from spaceone.core.pygrpc.server import GRPCServer
from .budget import Budget
from .budget_usage import BudgetUsage
from .cost import Cost
from .cost_query_set import CostQuerySet
from .data_source import DataSource
from .data_source_account import DataSourceAccount
from .data_source_rule import DataSourceRule
from .job import Job
from .job_task import JobTask
from .cost_report_config import CostReportConfig
from .cost_report import CostReport
from .cost_report_data import CostReportData
from .unified_cost import UnifiedCost
from .report_adjustment_policy import ReportAdjustmentPolicy
from .report_adjustment import ReportAdjustment

_all_ = ["app"]

app = GRPCServer()
app.add_service(Budget)
app.add_service(BudgetUsage)
app.add_service(Cost)
app.add_service(CostReportConfig)
app.add_service(CostReport)
app.add_service(CostReportData)
app.add_service(CostQuerySet)
app.add_service(DataSource)
app.add_service(DataSourceAccount)
app.add_service(DataSourceRule)
app.add_service(Job)
app.add_service(JobTask)
app.add_service(UnifiedCost)
app.add_service(ReportAdjustmentPolicy)
app.add_service(ReportAdjustment)
