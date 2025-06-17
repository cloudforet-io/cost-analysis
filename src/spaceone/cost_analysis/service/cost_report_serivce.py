import calendar
import datetime
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone
from typing import Tuple, Union

from mongoengine import QuerySet
from spaceone.core import config
from spaceone.core.service import *

from spaceone.cost_analysis.manager import DataSourceAccountManager
from spaceone.cost_analysis.manager.cost_report.cost_report_format_generator import (
    CostReportFormatGenerator,
)
from spaceone.cost_analysis.model.cost_report.database import CostReport
from spaceone.cost_analysis.model.cost_report_config.database import CostReportConfig
from spaceone.cost_analysis.model.cost_report.request import *
from spaceone.cost_analysis.model.cost_report.response import *
from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.cost_report_manager import CostReportManager
from spaceone.cost_analysis.manager.cost_report_data_manager import (
    CostReportDataManager,
)
from spaceone.cost_analysis.manager.unified_cost_manager import UnifiedCostManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.email_manager import EmailManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.service.cost_report_data_service import (
    CostReportDataService,
)
from spaceone.cost_analysis.manager.cost_report.adjustment_policy_applier import (
    AdjustmentPolicyApplier,
)

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
        self.unified_cost_mgr = UnifiedCostManager()
        self.cost_report_config_mgr = CostReportConfigManager()
        self.cost_report_mgr = CostReportManager()
        self.cost_report_data_mgr = CostReportDataManager()
        self.ds_account_mgr = DataSourceAccountManager()
        self.ds_mgr = DataSourceManager()
        self.identity_mgr = IdentityManager()
        self.is_within_adjustment_period = False
        self.is_done_report = False

    @transaction(exclude=["authentication", "authorization", "mutation"])
    def create_cost_report_by_cost_report_config(self, params: dict) -> None:
        """Create cost report by cost report config
        Args:
            params (dict): {}
        Returns:
            None
        """

        self.create_default_cost_report_config_for_all_domains()

        for cost_report_config_vo in self._get_all_cost_report_configs():
            try:
                cost_report_config_vo: CostReportConfig = cost_report_config_vo
                self.cost_report_mgr.push_creating_cost_report_job(
                    params={
                        "domain_id": cost_report_config_vo.domain_id,
                        "cost_report_config_id": cost_report_config_vo.cost_report_config_id,
                        "is_scheduled": True,
                    }
                )
            except Exception as e:
                _LOGGER.error(
                    f"[create_cost_report_by_cost_report_config] failed to create cost report ({cost_report_config_vo.cost_report_config_id}), {e}",
                    exc_info=True,
                )

    @transaction(exclude=["authentication", "authorization", "mutation"])
    def create_cost_report_by_cost_report_config_id(self, params: dict):
        self.create_cost_report(params)

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def send(self, params: CostReportSendRequest) -> None:
        """Send cost report
        Args:
            params (dict): {
                'cost_report_id': 'str',    # required
                'workspace_id': 'str',
                'domain_id': 'str',         # inject from auth
            }
        Returns:
            None
        """
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        conditions = {
            "cost_report_id": params.cost_report_id,
            "domain_id": domain_id,
            "status": "SUCCESS",
        }

        if workspace_id is not None:
            conditions.update({"workspace_id": workspace_id})

        cost_report_vos = self.cost_report_mgr.filter_cost_reports(**conditions)
        self.send_cost_report(cost_report_vos[0])

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get_url(self, params: CostReportGetUrlRequest) -> dict:
        """Get cost report url
        Args:
            params (dict): {
                'cost_report_id': 'str',    # required
                'workspace_id': 'str',
                'domain_id': 'str',         # inject from auth
            }
        Returns:

        """

        domain_id = params.domain_id
        cost_report_id = params.cost_report_id

        # check cost report config and cost report
        cost_report_vo = self.cost_report_mgr.get_cost_report(
            domain_id, cost_report_id, params.workspace_id
        )
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id, cost_report_vo.cost_report_config_id
        )

        workspace_id = cost_report_vo.workspace_id
        language = cost_report_config_vo.language

        sso_access_token = self._get_temporary_sso_access_token(domain_id, workspace_id)
        cost_report_link = self._get_console_cost_report_url(
            domain_id, cost_report_id, sso_access_token, language
        )

        return {"cost_report_link": cost_report_link, "domain_id": domain_id}

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get(self, params: CostReportGetRequest) -> Union[CostReportResponse, dict]:
        """Get cost report"""

        cost_report_vo = self.cost_report_mgr.get_cost_report(
            params.domain_id, params.cost_report_id, params.workspace_id
        )

        return CostReportResponse(**cost_report_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "cost_report_config_id",
            "status",
            "domain_id",
            "workspace_id",
            "cost_report_id",
        ]
    )
    @append_keyword_filter(
        [
            "report_number",
            "name",
            "report_year",
            "report_month",
        ]
    )
    @set_query_page_limit(default_limit=100)
    @convert_model
    def list(
        self, params: CostReportSearchQueryRequest
    ) -> Union[CostReportsResponse, dict]:
        """List cost reports"""

        query = params.query or {}

        if params.status and not any(_filter.get("k") == "status" for _filter in query.get("filter", [])):
            query.setdefault("filter", []).append({"k": "status", "v": "DONE", "o": "eq"})

        cost_report_vos, total_count = self.cost_report_mgr.list_cost_reports(query)

        cost_reports_info = [
            cost_report_vo.to_dict() for cost_report_vo in cost_report_vos
        ]
        return CostReportsResponse(results=cost_reports_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:CostReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(["cost_config_report_id", "domain_id"])
    @convert_model
    def analyze(self):
        pass

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
            "report_number",
            "name",
        ]
    )
    @convert_model
    def stat(self, params: CostReportDataStatQueryRequest) -> dict:
        """Stat cost reports"""

        query = params.query or {}
        return self.cost_report_mgr.stat_cost_reports(query)

    def create_cost_report(self, params: dict):
        domain_id = params["domain_id"]
        cost_report_config_id = params["cost_report_config_id"]
        is_scheduled = params.get("is_scheduled", False)

        config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id, cost_report_config_id
        )
        current_date = datetime.now(timezone.utc)
        adjustment_options = config_vo.adjustment_options or {}

        metadata = self._collect_cost_report_metadata(config_vo, current_date)

        create_in_progress_report, create_adjusting_report, report_month = (
            self._calculate_report_flags_and_report_month(
                metadata["issue_day"],
                metadata["is_last_day"],
                current_date,
                adjustment_options,
                is_scheduled,
                cost_report_config_id,
                domain_id,
            )
        )

        metadata["report_month"] = report_month
        metadata["currency"] = config_vo.currency

        issue_month, report_issue_day, issue_date = self._prepare_report_metadata(
            metadata, report_month
        )

        unified_costs = self.unified_cost_mgr.analyze_unified_cost_for_report(
            report_month=report_month,
            data_source_ids=metadata["data_source_ids"],
            domain_id=domain_id,
            workspace_ids=metadata["workspace_ids"],
            scope=config_vo.scope,
        )

        status = None
        if create_in_progress_report:
            status = "IN_PROGRESS"
        elif create_adjusting_report:
            status = "ADJUSTING"

        if status:
            _LOGGER.debug(
                f"[create_cost_report] status = {status}, issue_month={issue_month}, report_month={report_month}, report_issue_day={report_issue_day}")

            reports = self._build_cost_reports_from_costs(
                status, config_vo, unified_costs, metadata
            )

            self._persist_cost_reports_by_status(
                reports,
                report_month,
                report_issue_day,
                metadata["data_source_ids"],
                domain_id,
                issue_date,
            )

    def _get_all_cost_report_configs(self) -> QuerySet:
        return self.cost_report_config_mgr.filter_cost_report_configs(state="ENABLED")

    def _change_status_to_expired(
        self,
        domain_id: str,
        cost_report_config_id: str,
        report_month: str,
        cost_report_created_at: datetime,
    ):
        cost_report_update_query = {
            "filter": [
                {"k": "cost_report_config_id", "v": cost_report_config_id, "o": "eq"},
                {"k": "report_month", "v": report_month, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "status", "v": "DONE", "o": "eq"},
                {"k": "created_at", "v": cost_report_created_at, "o": "lt"},
            ]
        }

        cost_reports_vos, total_count = self.cost_report_mgr.list_cost_reports(
            cost_report_update_query
        )

        for cost_report_vo in cost_reports_vos:
            self.cost_report_mgr.update_cost_report_by_vo({"status": "EXPIRED"}, cost_report_vo)

    def _delete_old_cost_reports(
        self,
        report_month: str,
        domain_id: str,
        cost_report_config_id: str,
        cost_report_created_at: datetime,
    ) -> None:
        cost_report_delete_query = {
            "filter": [
                {"k": "cost_report_config_id", "v": cost_report_config_id, "o": "eq"},
                {"k": "report_month", "v": report_month, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "status", "v": ["DONE","EXPIRED"], "o": "not_in"},
                {"k": "created_at", "v": cost_report_created_at, "o": "lt"},
            ]
        }

        cost_reports_vos, total_count = self.cost_report_mgr.list_cost_reports(
            cost_report_delete_query
        )

        for cost_report_vo in cost_reports_vos:
            cost_report_data_vos = self.cost_report_data_mgr.filter_cost_reports_data(
                cost_report_config_id=cost_report_vo.cost_report_config_id,
                cost_report_id=cost_report_vo.cost_report_id,
                domain_id=cost_report_vo.domain_id,
            )
            _LOGGER.debug(
                f"[_delete_old_cost_reports] delete cost report data ({cost_report_config_id}:{cost_report_vo.cost_report_id}:{report_month}) (count = {len(cost_report_data_vos)})"
            )

            cost_report_data_vos.delete()
            self.cost_report_mgr.delete_cost_report_by_vo(cost_report_vo)

        _LOGGER.debug(
            f"[_delete_last_month_cost_reports] delete cost reports ({cost_report_config_id}:{report_month}) (count = {total_count}))"
        )

    def send_cost_report(self, cost_report_vo: CostReport) -> None:
        domain_id = cost_report_vo.domain_id
        workspace_id = cost_report_vo.workspace_id

        # Get Cost Report Config
        cost_report_config_id = cost_report_vo.cost_report_config_id
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id, cost_report_config_id
        )

        language = cost_report_config_vo.language
        recipients = cost_report_config_vo.recipients
        role_types = recipients.get("role_types", [])
        emails = recipients.get("emails", [])

        verified_users_info = self.get_email_verified_workspace_owner_users(
            domain_id, workspace_id, role_types
        )

        if emails:
            pass

        if verified_users_info:
            email_mgr = EmailManager()
            sso_access_token = self._get_temporary_sso_access_token(
                domain_id, workspace_id
            )
            for user_info in verified_users_info:
                try:
                    user_id = user_info["user_id"]
                    email = user_info.get("email", user_id)

                    cost_report_link = self._get_console_cost_report_url(
                        domain_id,
                        cost_report_vo.cost_report_id,
                        sso_access_token,
                        language,
                    )

                    email_mgr.send_cost_report_email(
                        user_id, email, cost_report_link, language, cost_report_vo
                    )
                except Exception as e:
                    _LOGGER.error(
                        f"[send_cost_report] failed to send cost report ({cost_report_vo.cost_report_id}) to {user_info['user_id']}({domain_id}), {e}"
                    )

        _LOGGER.debug(
            f"[send_cost_report] send cost report ({workspace_id}/{cost_report_vo.cost_report_id}) to {len(verified_users_info)} users"
        )

    def get_email_verified_workspace_owner_users(
        self, domain_id: str, workspace_id: str, role_types: list = None
    ) -> list:
        if "WORKSPACE_OWNER" not in role_types:
            return []

        # list users in workspace
        users_info = self.identity_mgr.list_workspace_users(
            params={
                "workspace_id": workspace_id,
                "state": "ENABLED",
                "role_type": "WORKSPACE_OWNER",
                "query": {
                    "filter": [
                        {"k": "email_verified", "v": True, "o": "eq"},
                    ]
                },
            },
            domain_id=domain_id,
        ).get("results", [])

        return users_info

    def get_start_cost_report_number(
        self, domain_id: str, issue_date: str = None
    ) -> int:
        return (
            self.cost_report_mgr.filter_cost_reports(
                domain_id=domain_id, issue_date=issue_date
            ).count()
            + 1
        )

    def _get_workspace_name_map(self, domain_id: str) -> list:
        workspace_ids = []
        workspaces = self.identity_mgr.list_workspaces(
            {"query": {"filter": [{"k": "state", "v": "ENABLED", "o": "eq"}]}},
            domain_id,
        )

        for workspace in workspaces.get("results", []):
            workspace_ids.append(workspace["workspace_id"])

        return workspace_ids

    def _get_console_cost_report_url(
        self, domain_id: str, cost_report_id: str, token: str, language: str
    ) -> str:
        domain_name = self._get_domain_name(domain_id)

        console_domain = config.get_global("EMAIL_CONSOLE_DOMAIN")
        console_domain = console_domain.format(domain_name=domain_name)

        return f"{console_domain}/cost-report-detail?sso_access_token={token}&cost_report_id={cost_report_id}&language={language}"

    def _get_domain_name(self, domain_id: str) -> str:
        domain_name = self.identity_mgr.get_domain_name(domain_id)
        return domain_name

    def _get_temporary_sso_access_token(self, domain_id: str, workspace_id: str) -> str:
        system_token = config.get_global("TOKEN")
        timeout = config.get_global("COST_REPORT_TOKEN_TIMEOUT", 259200)
        permissions = config.get_global(
            "COST_REPORT_DEFAULT_PERMISSIONS",
            [
                "cost-analysis:CostReport.read",
                "cost-analysis:CostReportData.read",
                "cost-analysis:CostReportConfig.read",
                "config:Domain.read",
                "identity:Provider.read",
            ],
        )

        params = {
            "grant_type": "SYSTEM_TOKEN",
            "scope": "WORKSPACE",
            "token": system_token,
            "workspace_id": workspace_id,
            "domain_id": domain_id,
            "timeout": timeout,
            "permissions": permissions,
        }
        return self.identity_mgr.grant_token(params)

    def _calculate_report_flags_and_report_month(
        self,
        issue_day: int,
        is_last_day: bool,
        current_date: datetime,
        adjustment_options: dict,
        is_scheduled: bool = False,
        cost_report_config_id: str = None,
        domain_id:str = None
    ) -> Tuple[bool, bool, str]:
        create_in_progress_report = False
        create_adjusting_report = False

        adjustment_state = adjustment_options.get("enabled", False)
        adjustment_period = adjustment_options.get("period", 0)
        self.is_within_adjustment_period = adjustment_state and adjustment_period > 0

        retry_days = min(config.get_global("COST_REPORT_RETRY_DAYS", 7), 10)
        retry_start_date = current_date - relativedelta(days=retry_days)

        if retry_start_date.month != current_date.month:
            issue_base_date = current_date - relativedelta(months=1)
            report_date = current_date - relativedelta(months=2)
        else:
            issue_base_date = current_date
            report_date = current_date - relativedelta(months=1)

        issue_day = self.get_issue_day(is_last_day, issue_day, issue_base_date)
        issue_date = issue_base_date.replace(day=issue_day).date()
        current_date = current_date.date()
        done_date = issue_date + relativedelta(days=adjustment_period)
        report_month = report_date.strftime("%Y-%m")

        if current_date < issue_date:
            create_in_progress_report = True
        else:
            create_adjusting_report = True
            if current_date >= done_date:
                if is_scheduled and self._check_done_cost_report_exist(
                        domain_id=domain_id,
                        cost_report_config_id=cost_report_config_id,
                        report_month=report_month,
                ):
                    create_adjusting_report = False
                else:
                    self.is_done_report = True

        return create_in_progress_report, create_adjusting_report, report_month

    def _check_done_cost_report_exist(
        self,
        domain_id: str,
        cost_report_config_id: str,
        report_month: str,
    ):
        cost_report_vos = self.cost_report_mgr.filter_cost_reports(
            cost_report_config_id=cost_report_config_id,
            status="DONE",
            domain_id=domain_id,
            report_month=report_month,
        )

        if cost_report_vos.count() > 0:
            return True
        else:
            return False

    @staticmethod
    def get_issue_day(
        is_last_day: bool, issue_day: int = None, current_date: datetime = None
    ) -> int:
        if not current_date:
            current_date = datetime.now(timezone.utc)

        current_year = current_date.year
        current_month = current_date.month

        _, last_day = calendar.monthrange(current_year, current_month)

        if is_last_day:
            return last_day
        else:
            return min(issue_day, last_day)

    @staticmethod
    def generate_report_number(
        report_month: str, issue_day: int, cost_report_idx: int
    ) -> str:
        report_date = f"{report_month}-{issue_day}"
        date_object = datetime.strptime(report_date, "%Y-%m-%d")

        return f"CostReport_{date_object.strftime('%y%m%d')}{str(cost_report_idx).zfill(4)}"

    def create_default_cost_report_config_for_all_domains(self):
        cost_report_config_mgr = CostReportConfigManager()

        params = {
            "query": {
                "filter": [{"k": "state", "v": "ENABLED", "o": "eq"}],
                "only": ["domain_id", "state"],
            }
        }

        response = self.identity_mgr.list_domains(params)
        domain_infos = response.get("results", [])

        for domain_info in domain_infos:
            domain_id = domain_info["domain_id"]
            cost_report_config_mgr.create_default_cost_report_config(domain_id)

    @staticmethod
    def _get_issue_month_from_report_month(report_month: str) -> str:
        """
        report_month: 2024-06
        """
        issue_month_datetime = datetime.strptime(report_month, "%Y-%m") + relativedelta(
            months=1
        )
        issue_month = issue_month_datetime.strftime("%Y-%m")
        return issue_month

    def _persist_cost_reports_by_status(
        self,
        cost_reports: Union[list, QuerySet],
        report_month,
        report_issue_day,
        data_source_ids,
        domain_id,
        issue_date,
    ):
        cost_report_data_svc = CostReportDataService()

        start_cost_report_number = self.get_start_cost_report_number(
            domain_id, issue_date
        )

        cost_report_config_id = None
        report_created_at = datetime.now(timezone.utc)
        for idx, report in enumerate(cost_reports, start=start_cost_report_number):
            report["report_number"] = self.generate_report_number(
                report_month, report_issue_day, idx
            )
            cost_report_config_id = report["cost_report_config_id"]

            cost_report_vo = self.cost_report_mgr.create_cost_report(report)
            cost_report_data_svc.create_cost_report_data(cost_report_vo, report)

            if cost_report_vo.status == "ADJUSTING":
                if self.is_within_adjustment_period:
                    _LOGGER.info(
                        f"[_persist_cost_reports_by_status] apply Adjustment Policy (cost_report_id={cost_report_vo.cost_report_id})"
                    )

                    adjustment_applier = AdjustmentPolicyApplier(
                        cost_report_vo,
                        data_source_ids,
                        report,
                    )

                    if adjustment_applier.is_applied:
                        adjustment_applier.apply_policies()
                        adjusted_cost = self._get_total_cost_applied_adjustment(
                            cost_report_vo
                        )

                        self.cost_report_mgr.update_cost_report_by_vo(
                            {
                                "cost": adjusted_cost,
                                "is_adjusted": True,
                            },
                            cost_report_vo,
                        )

                if self.is_done_report:
                    cost_report_vo = self._update_cost_report_done_status(cost_report_vo)
                    self.send_cost_report(cost_report_vo)

        if self._check_done_cost_report_exist(domain_id, cost_report_config_id, report_month):
            self._change_status_to_expired(domain_id, cost_report_config_id, report_month, report_created_at)

        self._delete_old_cost_reports(
            report_month,
            domain_id,
            cost_report_config_id,
            report_created_at
        )

    @staticmethod
    def _build_cost_reports_from_costs(
        status, config_vo, unified_costs, metadata
    ) -> list:
        generator = CostReportFormatGenerator(
            metadata=metadata,
            report_month=metadata["report_month"],
            scope=config_vo.scope,
            cost_report_config_id=config_vo.cost_report_config_id,
            domain_id=config_vo.domain_id,
        )
        return generator.make_cost_reports(unified_costs, status)

    def _collect_cost_report_metadata(
        self,
        config_vo: CostReportConfig,
        current_date: datetime,
    ) -> dict:
        domain_id = config_vo.domain_id
        data_source_filter = config_vo.data_source_filter or {}
        is_last_day = config_vo.is_last_day or False
        issue_day = self.get_issue_day(is_last_day, config_vo.issue_day, current_date)
        current_month = current_date.strftime("%Y-%m")

        workspace_ids = self._get_workspace_name_map(domain_id)

        data_source_ids = self.ds_mgr.get_data_source_ids(
            domain_id, workspace_ids, data_source_filter
        )

        return {
            "domain_id": domain_id,
            "workspace_ids": workspace_ids,
            "data_source_ids": data_source_ids,
            "is_last_day": is_last_day,
            "issue_day": issue_day,
            "current_month": current_month,
        }

    def _prepare_report_metadata(
        self, context: dict, report_month: str
    ) -> tuple[str, int, str]:
        issue_month = self._get_issue_month_from_report_month(report_month)
        report_issue_day = self.get_issue_day(
            context["is_last_day"],
            context["issue_day"],
            datetime.strptime(report_month, "%Y-%m"),
        )
        issue_date = f"{issue_month}-{str(context['issue_day']).zfill(2)}"
        return issue_month, report_issue_day, issue_date

    def get_project_name_map(
        self, domain_id: str, workspace_ids: list
    ) -> Tuple[dict, list]:
        project_name_map = {}
        project_ids = []
        projects = self.identity_mgr.list_projects(
            params={
                "query": {
                    "filter": [
                        {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                    ]
                }
            },
            domain_id=domain_id,
        )
        for project in projects.get("results", []):
            project_name_map[project["project_id"]] = project["name"]
            project_ids.append(project["project_id"])
        return project_name_map, project_ids

    def _get_total_cost_applied_adjustment(self, cost_report_vo: CostReport) -> dict:
        cost_info = {}

        domain_id = cost_report_vo.domain_id
        workspace_id = cost_report_vo.workspace_id
        cost_report_id = cost_report_vo.cost_report_id
        report_month = cost_report_vo.report_month

        currencies = config.get_global("SUPPORTED_CURRENCIES", ["USD", "KRW", "JPY"])

        query = {
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "cost_report_id", "v": cost_report_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                {"k": "report_year", "v": report_month.split("-")[0], "o": "eq"},
                {"k": "report_month", "v": report_month, "o": "eq"},
            ],
        }

        if project_id := cost_report_vo.project_id:
            query["filter"].append({"k": "project_id", "v": project_id, "o": "eq"})

        if service_account_id := cost_report_vo.service_account_id:
            query["filter"].append(
                {"k": "service_account_id", "v": service_account_id, "o": "eq"}
            )

        fields = {
            f"cost_{currency}": {"key": f"cost.{currency}", "operator": "sum"}
            for currency in currencies
        }
        query["fields"] = fields

        _LOGGER.debug(f"[analyze_cost_reports_data] query: {query}")
        response = self.cost_report_data_mgr.analyze_cost_reports_data(query)
        results = response.get("results", [])

        for key, value in results[0].items():
            if key.startswith("cost_"):
                currency = key.replace("cost_", "")
                cost_info[currency] = value or 0.0
        return cost_info

    def _update_cost_report_done_status(self, cost_report_vo: CostReport) -> CostReport:
        domain_id = cost_report_vo.domain_id
        report_month = cost_report_vo.report_month

        cost_report_vo = self.cost_report_mgr.update_cost_report_by_vo(
            {"status": "DONE"}, cost_report_vo
        )

        adjusted_cost_data_vos = self.cost_report_data_mgr.filter_cost_reports_data(
            domain_id=domain_id,
            cost_report_id=cost_report_vo.cost_report_id,
            report_month=report_month,
            is_confirmed=False,
        )
        for adjusted_cost_data_vo in adjusted_cost_data_vos:
            self.cost_report_data_mgr.update_cost_report_data_by_vo(
                {"is_confirmed": True}, adjusted_cost_data_vo
            )

        return cost_report_vo
