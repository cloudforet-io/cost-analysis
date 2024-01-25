import copy
import calendar
import datetime
import logging
from datetime import datetime

from spaceone.core.service import *
from spaceone.cost_analysis.model.cost_report.database import CostReport
from spaceone.cost_analysis.model.cost_report_config.database import CostReportConfig
from spaceone.cost_analysis.model.cost_report.request import *
from spaceone.cost_analysis.model.cost_report.response import *
from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.cost_report_manager import CostReportManager
from spaceone.cost_analysis.manager.email_manager import EmailManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostReportService(BaseService):
    resource = "CostReport"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr = CostManager()
        self.cost_report_config_mgr = CostReportConfigManager()
        self.cost_report_mgr = CostReportManager()

    @transaction(exclude=["authentication", "authorization", "mutation"])
    def create_cost_report_by_cost_report_config(self, params: dict):
        """Create cost report by cost report config"""

        for cost_report_config_id in self._get_all_cost_report_configs():
            self.create_cost_report(cost_report_config_id)

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def send(self, params: CostReportSendRequest) -> None:
        """Send cost report"""
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        cost_report_vo = self.cost_report_mgr.filter_cost_reports(
            cost_report_id=params.cost_report_id,
            domain_id=domain_id,
            workspace_id=workspace_id,
            status="SUCCESS",
        )

        # Get Cost Report Config
        cost_report_config_id = cost_report_vo.cost_report_config_id
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            cost_report_config_id, domain_id, workspace_id
        )

        recipients = cost_report_config_vo.recipients
        role_types = recipients.get("role_types", [])
        emails = recipients.get("emails", [])

        # list workspace owner role bindings
        identity_mgr = IdentityManager()

        workspace_ids = []
        if workspace_id is not None:
            rb_query = {
                "filter": [
                    {"k": "role_type", "v": role_types, "o": "in"},
                    {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                ],
            }
            role_bindings_info = identity_mgr.list_role_bindings(
                params={"query": rb_query}, domain_id=domain_id
            )

            workspace_ids = [
                role_binding_info["workspace_id"]
                for role_binding_info in role_bindings_info
            ]
            workspace_ids = list(set(workspace_ids))
        else:
            workspace_ids.append(workspace_id)

        # list workspace owner users
        email_mgr = EmailManager()
        for ws_id in workspace_ids:
            users_info = identity_mgr.list_workspace_users(
                params={"workspace_id": ws_id, "state": "ENABLED"}, domain_id=domain_id
            )
            for user_info in users_info:
                user_id = user_info["user_id"]
                email = user_info.get("email", user_id)
                email_mgr.send_cost_report_email(user_id, email)

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get(self, params: CostReportGetRequest) -> CostReportResponse:
        """Get cost report"""

        cost_report_vo = self.cost_report_mgr.get_cost_report(
            params.cost_report_id, params.domain_id, params.workspace_id
        )

        return CostReportResponse(**cost_report_vo.to_dict())

    def create_cost_report(self, cost_report_config_id: str):
        pass

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "state",
            "cost_report_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(
        [
            "cost_report_number",
            "workspace_name",
            "report_year" "report_month",
        ]
    )
    @convert_model
    def list(self, params: CostReportSearchQueryRequest) -> CostReportsResponse:
        """List cost reports"""

        query = params.query or {}

        cost_report_vos, total_count = self.cost_report_mgr.list_cost_reports(query)

        cost_reports_info = [
            cost_report_vo.to_dict() for cost_report_vo in cost_report_vos
        ]
        return CostReportsResponse(results=cost_reports_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "cost_report_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(
        [
            "cost_report_number",
            "workspace_name",
        ]
    )
    @convert_model
    def stat(self, params: CostReportDataStatQueryRequest) -> dict:
        """Stat cost reports"""

        return self.cost_report_mgr.stat_cost_reports(params.query)

    def _aggregate_monthly_cost_report(self, cost_report_config_vo: CostReportConfig):
        issue_day = cost_report_config_vo.issue_day
        report_month = self._get_report_month()
        data_sources = cost_report_config_vo.data_source_filter.get("data_sources", [])
        domain_id = cost_report_config_vo.domain_id

        query = {
            "group_by": [
                "workspace_id",
                "billed_year",
            ],
            "fields": {
                "cost": {"key": "cost", "operator": "sum"},
            },
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ],
        }
        if data_sources:
            query["filter"].append(
                {"k": "data_source_id", "v": data_sources, "o": "in"}
            )

        response = self.cost_mgr.analyze_costs(query, domain_id, target="PRIMARY")

    def _get_all_cost_report_configs(self) -> CostReportConfig:
        return self.cost_report_config_mgr.list_cost_reports_config(state="ENABLED")

    def _get_report_month(self):
        return datetime.now().strftime("%Y-%m")

    @staticmethod
    def _get_issue_day(cost_report_config_vo: CostReportConfig):
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        _, last_day = calendar.monthrange(current_year, current_month)

        if cost_report_config_vo.is_last_day:
            return last_day

        else:
            return min(cost_report_config_vo.issue_day, last_day)
