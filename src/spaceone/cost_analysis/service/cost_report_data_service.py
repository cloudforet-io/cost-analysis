import calendar
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Union, Tuple
from mongoengine import QuerySet

from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.cost_report_data_manager import (
    CostReportDataManager,
)
from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.cost_report_data.request import *
from spaceone.cost_analysis.model.cost_report_data.response import *
from spaceone.cost_analysis.model.cost_report_config.database import CostReportConfig

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostReportDataService(BaseService):
    resource = "CostReportData"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr = CostManager()
        self.cost_report_data_mgr = CostReportDataManager()

    @transaction(exclude=["authentication", "authorization", "mutation"])
    def create_cost_report_data_by_cost_report_config(self, params: dict) -> None:
        """Create cost report by cost report config"""

        for cost_report_config_vo in self._get_all_cost_report_configs():
            issue_day = self._get_issue_day(cost_report_config_vo)
            if issue_day == datetime.utcnow().day:
                self.create_cost_report_data(cost_report_config_vo)

    @transaction(
        permission="cost-analysis:CostReportData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "cost_report_config_id",
            "cost_report_data_id",
            "product",
            "provider",
            "is_confirmed",
            "data_source_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["product", "cost_report_data_id"])
    @convert_model
    def list(
        self, params: CostReportDataSearchQueryRequest
    ) -> Union[CostReportsDataResponse, dict]:
        """List cost report data"""

        query = params.query or {}
        (
            cost_report_data_vos,
            total_count,
        ) = self.cost_report_data_mgr.list_cost_reports_data(query)

        cost_reports_data_info = [
            cost_report_data_vo.to_dict()
            for cost_report_data_vo in cost_report_data_vos
        ]
        return CostReportsDataResponse(
            results=cost_reports_data_info, total_count=total_count
        )

    @transaction(
        permission="cost-analysis:CostReportData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "product",
            "provider",
            "is_confirmed",
            "cost_report_config_id",
            "cost_report_data_id",
            "data_source_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(
        ["provider", "product", "workspace_name", "project_name", "cost_report_data_id"]
    )
    @set_query_page_limit(1000)
    @convert_model
    def analyze(self, params: CostReportDataAnalyzeQueryRequest) -> dict:
        """Analyze cost report data"""

        query = params.query or {}

        return self.cost_report_data_mgr.analyze_cost_reports_data(
            query, target="PRIMARY"
        )

    @transaction(
        permission="cost-analysis:CostReportData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "cost_report_config_id",
            "cost_report_data_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @convert_model
    def stat(self, params: CostReportDataStatQueryRequest) -> dict:
        """Analyze cost report data"""

        query = params.query or {}
        return self.cost_report_data_mgr.stat_cost_reports_data(query)

    @staticmethod
    def _get_all_cost_report_configs() -> QuerySet:
        cost_report_config_mgr = CostReportConfigManager()
        return cost_report_config_mgr.filter_cost_report_configs(state="ENABLED")

    def create_cost_report_data(self, cost_report_config_vo: CostReportConfig):
        domain_id = cost_report_config_vo.domain_id
        data_source_filter = cost_report_config_vo.data_source_filter or {}

        workspace_name_map, workspace_ids = self._get_workspace_name_map(domain_id)
        data_source_currency_map, data_source_ids = self._get_data_source_currency_map(
            data_source_filter, workspace_ids, domain_id
        )
        project_name_map = self._get_project_name_map(workspace_ids, domain_id)
        service_account_name_map = self._get_service_account_name_map(
            workspace_ids, domain_id
        )

        current_month, last_month = self._get_current_and_last_month()
        issue_day = self._get_issue_day(cost_report_config_vo)

        if issue_day == datetime.utcnow().day:
            self._aggregate_monthly_cost_report_data(
                cost_report_config_vo=cost_report_config_vo,
                workspace_name_map=workspace_name_map,
                workspace_ids=workspace_ids,
                project_name_map=project_name_map,
                service_account_name_map=service_account_name_map,
                data_source_currency_map=data_source_currency_map,
                data_source_ids=data_source_ids,
                report_month=last_month,
                issue_day=issue_day,
                is_confirmed=True,
            )

        self._aggregate_monthly_cost_report_data(
            cost_report_config_vo=cost_report_config_vo,
            workspace_name_map=workspace_name_map,
            workspace_ids=workspace_ids,
            project_name_map=project_name_map,
            service_account_name_map=service_account_name_map,
            data_source_currency_map=data_source_currency_map,
            data_source_ids=data_source_ids,
            report_month=current_month,
            issue_day=issue_day,
        )

    def _aggregate_monthly_cost_report_data(
        self,
        cost_report_config_vo: CostReportConfig,
        workspace_name_map: dict,
        workspace_ids: list,
        project_name_map: dict,
        service_account_name_map: dict,
        data_source_currency_map: dict,
        data_source_ids: list,
        report_month: str,
        issue_day: int,
        is_confirmed: bool = False,
    ):
        domain_id = cost_report_config_vo.domain_id
        currency = cost_report_config_vo.currency
        report_year = report_month.split("-")[0]

        query = {
            "group_by": [
                "billed_year",
                "workspace_id",
                "project_id",
                "data_source_id",
                "service_account_id",
                "data_source_id",
                "product",
                "provider",
            ],
            "fields": {
                "cost": {"key": "cost", "operator": "sum"},
            },
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "billed_year", "v": report_year, "o": "eq"},
                {"k": "billed_month", "v": report_month, "o": "eq"},
                {"k": "data_source_id", "v": data_source_ids, "o": "in"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
            ],
        }

        _LOGGER.debug(f"[aggregate_monthly_cost_report] query: {query}")
        response = self.cost_mgr.analyze_monthly_costs(
            query, domain_id, target="PRIMARY"
        )

        results = response.get("results", [])
        currency_mgr = CurrencyManager()
        for aggregated_cost_report in results:
            ag_cost_report_currency = data_source_currency_map.get(
                aggregated_cost_report.pop("data_source_id")
            )
            aggregated_cost_report["cost"] = {
                ag_cost_report_currency: aggregated_cost_report.pop("cost")
            }
            aggregated_cost_report["currency"] = currency
            aggregated_cost_report["issue_date"] = f"{report_month}-{issue_day}"
            aggregated_cost_report["report_month"] = report_month
            aggregated_cost_report["report_year"] = aggregated_cost_report.pop(
                "billed_year"
            )
            aggregated_cost_report["workspace_name"] = workspace_name_map.get(
                aggregated_cost_report["workspace_id"], "Unknown"
            )
            aggregated_cost_report["project_name"] = project_name_map.get(
                aggregated_cost_report["project_id"], "Unknown"
            )
            aggregated_cost_report[
                "service_account_name"
            ] = service_account_name_map.get(
                aggregated_cost_report["service_account_id"], "Unknown"
            )

            aggregated_cost_report[
                "cost_report_config_id"
            ] = cost_report_config_vo.cost_report_config_id
            aggregated_cost_report["domain_id"] = domain_id
            aggregated_cost_report["is_confirmed"] = is_confirmed

            aggregated_cost_report["cost"] = currency_mgr.convert_exchange_rate(
                aggregated_cost_report
            ).get("cost")

            self.cost_report_data_mgr.create_cost_report_data(aggregated_cost_report)

        _LOGGER.debug(
            f"[aggregate_monthly_cost_report] create cost report ({report_month}) (count = {len(results)})"
        )

    @staticmethod
    def _get_current_and_last_month() -> Tuple[str, str]:
        current_month = datetime.utcnow().strftime("%Y-%m")
        last_month = (datetime.utcnow() - relativedelta(months=1)).strftime("%Y-%m")
        return current_month, last_month

    @staticmethod
    def _get_issue_day(cost_report_config_vo: CostReportConfig) -> int:
        current_date = datetime.utcnow()
        current_year = current_date.year
        current_month = current_date.month

        _, last_day = calendar.monthrange(current_year, current_month)

        if cost_report_config_vo.is_last_day:
            return last_day
        else:
            return min(cost_report_config_vo.issue_day, last_day)

    def _get_workspace_name_map(self, domain_id: str) -> Tuple[dict, list]:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        workspace_name_map = {}
        workspaces = identity_mgr.list_workspaces(
            {"query": {"filter": [{"k": "state", "v": "ENABLED", "o": "eq"}]}},
            domain_id,
        )
        workspace_ids = []
        for workspace in workspaces.get("results", []):
            workspace_name_map[workspace["workspace_id"]] = workspace["name"]
            workspace_ids.append(workspace["workspace_id"])
        return workspace_name_map, workspace_ids

    @staticmethod
    def _get_data_source_currency_map(
        data_source_filter: dict, workspace_ids, domain_id: str
    ) -> Tuple[dict, list]:
        data_source_currency_map = {}
        data_source_mgr = DataSourceManager()

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
            ]
        }

        if data_sources := data_source_filter.get("data_sources"):
            query["filter"].append(
                {"k": "data_source_id", "v": data_sources, "o": "in"},
            )

        if data_source_state := data_source_filter.get("state", "ENABLED"):
            query["filter"].append({"k": "state", "v": data_source_state, "o": "eq"})

        _LOGGER.debug(f"[get_data_source_currency_map] query: {query}")

        data_source_vos, total_count = data_source_mgr.list_data_sources(query)
        data_source_ids = []
        for data_source_vo in data_source_vos:
            data_source_currency_map[
                data_source_vo.data_source_id
            ] = data_source_vo.plugin_info["metadata"]["currency"]
            data_source_ids.append(data_source_vo.data_source_id)

        return data_source_currency_map, data_source_ids

    def _get_project_name_map(self, workspace_ids, domain_id: str) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        project_name_map = {}
        projects = identity_mgr.list_projects(
            {
                "query": {
                    "filter": [
                        {"k": "domain_id", "v": domain_id, "o": "eq"},
                        {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                    ]
                }
            },
            domain_id,
        )
        for project in projects.get("results", []):
            project_name_map[project["project_id"]] = project["name"]
        return project_name_map

    def _get_service_account_name_map(self, workspace_ids, domain_id: str) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        service_account_name_map = {}
        service_accounts = identity_mgr.list_service_accounts(
            {
                "filter": [
                    {"k": "domain_id", "v": domain_id, "o": "eq"},
                    {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                ]
            },
            domain_id,
        )
        for service_account in service_accounts.get("results", []):
            service_account_name_map[
                service_account["service_account_id"]
            ] = service_account["name"]
        return service_account_name_map
