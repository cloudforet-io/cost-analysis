import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager import DataSourceAccountManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.cost_report_data_manager import (
    CostReportDataManager,
)
from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.unified_cost_manager import UnifiedCostManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.cost_report.database import CostReport
from spaceone.cost_analysis.model.cost_report_data.request import *
from spaceone.cost_analysis.model.cost_report_data.response import *

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
        self.unified_cost_mgr = UnifiedCostManager()
        self.cost_report_data_mgr = CostReportDataManager()
        self.ds_account_mgr = DataSourceAccountManager()

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
    @set_query_page_limit(1000)
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
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        [
            "product",
            "provider",
            "is_confirmed",
            "cost_report_config_id",
            "cost_report_id",
            "cost_report_data_id",
            "data_source_id",
            "workspace_id",
            "domain_id",
            "project_id",
            "user_projects",
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

        return self.cost_report_data_mgr.analyze_cost_reports_data(query)

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

    def create_cost_report_data(self, cost_report_vo: CostReport, unified_cost: dict):
        cost_report_config_mgr = CostReportConfigManager()

        domain_id = cost_report_vo.domain_id
        workspace_id = cost_report_vo.workspace_id
        cost_report_config_id = cost_report_vo.cost_report_config_id
        cost_report_id = cost_report_vo.cost_report_id
        report_month = cost_report_vo.report_month
        issue_date = cost_report_vo.issue_date
        workspace_name = self._get_workspace_name(domain_id, workspace_id)
        is_confirmed = True if cost_report_vo.status == "DONE" else False

        cost_report_config_vo = cost_report_config_mgr.get_cost_report_config(
            domain_id=domain_id, cost_report_config_id=cost_report_config_id
        )
        data_source_filter = cost_report_config_vo.data_source_filter or {}

        data_source_ids = self._list_data_source_ids_from_data_source(
            data_source_filter, workspace_id, domain_id
        )
        project_name_map = self._get_project_name_map(workspace_id, domain_id)
        service_account_name_map = self._get_service_account_name_map(
            workspace_id, domain_id
        )

        self._aggregate_unified_cost_report_data(
            domain_id=domain_id,
            workspace_id=workspace_id,
            cost_report_config_id=cost_report_config_id,
            cost_report_id=cost_report_id,
            workspace_name=workspace_name,
            project_name_map=project_name_map,
            service_account_name_map=service_account_name_map,
            data_source_ids=data_source_ids,
            report_month=report_month,
            issue_date=issue_date,
            is_confirmed=is_confirmed,
            unified_cost=unified_cost,
        )

    def _aggregate_unified_cost_report_data(
        self,
        domain_id: str,
        workspace_id: str,
        cost_report_config_id,
        cost_report_id: str,
        workspace_name: str,
        project_name_map: dict,
        service_account_name_map: dict,
        data_source_ids: list,
        report_month: str,
        issue_date: str,
        is_confirmed: bool = False,
        unified_cost: dict = None,
    ):
        report_year = report_month.split("-")[0]
        currencies = ["KRW", "USD", "JPY"]

        query = {
            "group_by": [
                "billed_year",
                "workspace_id",
                "project_id",
                "service_account_id",
                "data_source_id",
                "product",
                "provider",
            ],
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "data_source_id", "v": data_source_ids, "o": "in"},
                {"k": "billed_year", "v": report_year, "o": "eq"},
                {"k": "billed_month", "v": report_month, "o": "eq"},
            ],
        }

        fields = {
            f"cost_{currency}": {"key": f"cost.{currency}", "operator": "sum"}
            for currency in currencies
        }
        query["fields"] = fields

        v_workspace_ids = self._get_virtual_workspace_ids(domain_id, workspace_id)
        if v_workspace_ids:
            query["filter"].append(
                {"k": "workspace_id", "v": [workspace_id] + v_workspace_ids, "o": "in"}
            )
        else:
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        _LOGGER.debug(f"[aggregate_monthly_cost_report_data] query: {query}")
        response = self.unified_cost_mgr.analyze_unified_costs(query, domain_id)

        results = response.get("results", [])
        for aggregated_cost_report_data in results:
            aggregated_cost_report_data["cost"] = self._extract_cost_by_currency(
                aggregated_cost_report_data
            )
            aggregated_cost_report_data["issue_date"] = issue_date
            aggregated_cost_report_data["report_month"] = report_month
            aggregated_cost_report_data["report_year"] = (
                aggregated_cost_report_data.pop("billed_year")
            )
            aggregated_cost_report_data["workspace_name"] = workspace_name
            aggregated_cost_report_data["project_name"] = project_name_map.get(
                aggregated_cost_report_data["project_id"], ""
            )
            aggregated_cost_report_data["service_account_name"] = (
                service_account_name_map.get(
                    aggregated_cost_report_data.get("service_account_id"), ""
                )
            )

            aggregated_cost_report_data["cost_report_config_id"] = cost_report_config_id
            aggregated_cost_report_data["cost_report_id"] = cost_report_id
            aggregated_cost_report_data["workspace_id"] = workspace_id
            aggregated_cost_report_data["domain_id"] = domain_id
            aggregated_cost_report_data["is_confirmed"] = is_confirmed

            aggregated_cost_report_data["usage_type"] = unified_cost.get("usage_type")
            aggregated_cost_report_data["usage_unit"] = unified_cost.get("usage_unit")
            aggregated_cost_report_data["region_key"] = unified_cost.get("region_key")
            aggregated_cost_report_data["region_code"] = unified_cost.get("region_code")

            self.cost_report_data_mgr.create_cost_report_data(
                aggregated_cost_report_data
            )

        _LOGGER.debug(
            f"[aggregate_monthly_cost_report] create cost report data({report_month}) (count = {len(results)})"
        )

    def _get_workspace_name(self, domain_id: str, workspace_id: str) -> str:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        return identity_mgr.get_workspace(
            domain_id=domain_id, workspace_id=workspace_id
        )

    def _get_project_name_map(self, workspace_id: str, domain_id: str) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        project_name_map = {}
        response = identity_mgr.list_projects(
            {
                "query": {
                    "filter": [
                        {"k": "domain_id", "v": domain_id, "o": "eq"},
                        {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                    ]
                }
            },
            domain_id,
        )
        for project in response.get("results", []):
            project_name_map[project["project_id"]] = project["name"]
        return project_name_map

    def _get_service_account_name_map(self, workspace_id: str, domain_id: str) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        service_account_name_map = {}
        service_accounts = identity_mgr.list_service_accounts(
            {
                "filter": [
                    {"k": "domain_id", "v": domain_id, "o": "eq"},
                    {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                ]
            },
            domain_id,
        )
        for service_account in service_accounts.get("results", []):
            service_account_name_map[service_account["service_account_id"]] = (
                service_account["name"]
            )
        return service_account_name_map

    @staticmethod
    def _list_data_source_ids_from_data_source(
        data_source_filter: dict, workspace_id: str, domain_id: str
    ) -> list:
        data_source_mgr = DataSourceManager()

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": [workspace_id, "*"], "o": "in"},
            ]
        }

        if data_sources := data_source_filter.get("data_sources", []):
            query["filter"].append(
                {"k": "data_source_id", "v": data_sources, "o": "in"},
            )

        if data_source_state := data_source_filter.get("state", "ENABLED"):
            query["filter"].append(
                {"k": "schedule.state", "v": data_source_state, "o": "eq"}
            )
        _LOGGER.debug(f"[_list_data_source_ids_from_data_source] query: {query}")

        data_source_vos, total_count = data_source_mgr.list_data_sources(query)
        return [data_source_vo.data_source_id for data_source_vo in data_source_vos]

    def _get_virtual_workspace_ids(self, domain_id: str, workspace_id: str) -> list:
        v_workspace_ids = []
        ds_account_vos = self.ds_account_mgr.filter_data_source_accounts(
            domain_id=domain_id, workspace_id=workspace_id
        )

        for ds_account_vo in ds_account_vos:
            v_workspace_ids.append(ds_account_vo.v_workspace_id)

        return v_workspace_ids

    @staticmethod
    def _extract_cost_by_currency(cost_data: dict) -> dict:
        cost_dict = {}

        for key, value in cost_data.items():
            if key.startswith("cost_"):
                currency = key.replace("cost_", "")
                cost_dict[currency] = value

        return cost_dict
