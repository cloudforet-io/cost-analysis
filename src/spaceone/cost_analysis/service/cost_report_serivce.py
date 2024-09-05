import calendar
import datetime
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime
from typing import Tuple, Union

from mongoengine import QuerySet
from spaceone.core import config
from spaceone.core.service import *

from spaceone.cost_analysis.manager import DataSourceAccountManager
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
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.email_manager import EmailManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.service.cost_report_data_service import (
    CostReportDataService,
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
        self.cost_report_config_mgr = CostReportConfigManager()
        self.cost_report_mgr = CostReportManager()
        self.cost_report_data_mgr = CostReportDataManager()
        self.ds_account_mgr = DataSourceAccountManager()
        self.currency_map: Union[dict, None] = None
        self.currency_date: Union[str, None] = None

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
            "workspace_name",
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

        if params.status is None:
            query["filter"].append({"k": "status", "v": "SUCCESS", "o": "eq"})

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
            "workspace_name",
        ]
    )
    @convert_model
    def stat(self, params: CostReportDataStatQueryRequest) -> dict:
        """Stat cost reports"""

        query = params.query or {}
        return self.cost_report_mgr.stat_cost_reports(query)

    def create_cost_report(self, params: dict):
        cost_report_config_id = params["cost_report_config_id"]
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params["domain_id"], cost_report_config_id
        )
        domain_id = cost_report_config_vo.domain_id
        currency = cost_report_config_vo.currency
        data_source_filter = cost_report_config_vo.data_source_filter or {}
        is_last_day = cost_report_config_vo.is_last_day or False
        issue_day = self.get_issue_day(is_last_day, cost_report_config_vo.issue_day)

        current_date = datetime.utcnow()
        currency_date = current_date
        current_month = current_date.strftime("%Y-%m")

        _LOGGER.debug(
            f"[create_cost_report] start to create cost report by cost_report_config({cost_report_config_id})"
        )

        workspace_name_map, workspace_ids = self._get_workspace_name_map(domain_id)
        (
            data_source_currency_map,
            data_source_ids,
        ) = self._get_data_source_currency_map(
            domain_id, workspace_ids, data_source_filter
        )

        (
            is_create_report,
            report_month,
        ) = self._get_is_create_report_and_report_month(
            issue_day, is_last_day, current_date, domain_id, cost_report_config_id
        )

        currency_mgr = CurrencyManager()

        if is_create_report:
            issue_month = self._get_issue_month_fom_report_month(report_month)
            report_issue_day = self.get_issue_day(is_last_day, issue_day)

            _LOGGER.debug(
                f"[create_cost_report] issue_month {issue_month}, report_month {report_month} , issue_day {report_issue_day}"
            )

            currency_end_date = self._get_currency_date_from_report_month(
                report_month, issue_day
            )

            (
                currency_map,
                currency_date,
            ) = currency_mgr.get_currency_map_date(currency_end_date=currency_end_date)

            self.currency_map = currency_map
            self.currency_date = currency_date

            _LOGGER.debug(
                f"[create_cost_report] set currency date {self.currency_date} , currency map ({self.currency_map})"
            )

            cost_report_created_at = datetime.utcnow()
            self._aggregate_monthly_cost_report(
                domain_id=domain_id,
                workspace_ids=workspace_ids,
                data_source_ids=data_source_ids,
                cost_report_config_id=cost_report_config_id,
                workspace_name_map=workspace_name_map,
                data_source_currency_map=data_source_currency_map,
                report_month=report_month,
                currency=currency,
                issue_day=report_issue_day,
                status="SUCCESS",
                issue_month=issue_month,
            )

            self._delete_old_cost_reports(
                report_month,
                domain_id,
                cost_report_config_id,
                "SUCCESS",
                cost_report_created_at,
            )

            self._delete_old_cost_reports(
                report_month,
                domain_id,
                cost_report_config_id,
                "IN_PROGRESS",
                cost_report_created_at,
            )

        (
            currency_map,
            currency_date,
        ) = currency_mgr.get_currency_map_date(currency_end_date=currency_date)
        self.currency_map = currency_map
        self.currency_date = currency_date

        cost_report_created_at = datetime.utcnow()

        self._aggregate_monthly_cost_report(
            domain_id=domain_id,
            workspace_ids=workspace_ids,
            data_source_ids=data_source_ids,
            cost_report_config_id=cost_report_config_id,
            workspace_name_map=workspace_name_map,
            data_source_currency_map=data_source_currency_map,
            report_month=current_month,
            currency=currency,
            issue_day=current_date.day,
            status="IN_PROGRESS",
            issue_month=current_month,
        )

        self._delete_old_cost_reports(
            current_month,
            domain_id,
            cost_report_config_id,
            "IN_PROGRESS",
            cost_report_created_at,
        )

    def _aggregate_monthly_cost_report(
        self,
        domain_id: str,
        cost_report_config_id: str,
        workspace_name_map: dict,
        workspace_ids: list,
        data_source_currency_map: dict,
        data_source_ids: list,
        report_month: str,
        currency: str,
        issue_day: int,
        status: str,
        issue_month: str = None,
    ) -> None:
        report_year = report_month.split("-")[0]
        issue_date = f"{issue_month}-{str(issue_day).zfill(2)}"

        # collect enabled data_sources cost data
        query = {
            "group_by": ["workspace_id", "billed_year", "data_source_id"],
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
            ],
        }

        v_workspace_ids, v_workspace_id_map = self._get_virtual_workspace_ids_and_map(
            domain_id, workspace_ids
        )
        if v_workspace_ids:
            workspace_ids.extend(v_workspace_ids)

        query["filter"].append({"k": "workspace_id", "v": workspace_ids, "o": "in"})

        _LOGGER.debug(f"[aggregate_monthly_cost_report] query: {query}")
        response = self.cost_mgr.analyze_monthly_costs(query, domain_id)
        results = response.get("results", [])

        for aggregated_cost_report in results:
            aggregated_cost_report["workspace_id"] = v_workspace_id_map.get(
                aggregated_cost_report["workspace_id"],
                aggregated_cost_report["workspace_id"],
            )

            _ag_cost = aggregated_cost_report.pop("cost", 0.0)
            ag_cost_report_currency = data_source_currency_map.get(
                aggregated_cost_report.pop("data_source_id")
            )

            aggregated_cost_report["cost"] = CostReportManager.get_exchange_currency(
                _ag_cost, ag_cost_report_currency, self.currency_map
            )
            aggregated_cost_report["status"] = status
            aggregated_cost_report["currency"] = currency
            aggregated_cost_report["issue_date"] = issue_date
            aggregated_cost_report["report_month"] = report_month
            aggregated_cost_report["report_year"] = aggregated_cost_report.pop(
                "billed_year"
            )
            aggregated_cost_report["workspace_name"] = workspace_name_map.get(
                aggregated_cost_report["workspace_id"], "Unknown"
            )
            aggregated_cost_report["bank_name"] = "Yahoo! Finance"  # todo : replace
            aggregated_cost_report["cost_report_config_id"] = cost_report_config_id
            aggregated_cost_report["domain_id"] = domain_id

        aggregated_cost_report_results = self._aggregate_result_by_currency(results)

        cost_report_data_svc = CostReportDataService()
        cost_report_data_svc.currency_map = self.currency_map
        cost_report_data_svc.currency_date = self.currency_date

        start_cost_report_number = self.get_start_cost_report_number(
            domain_id, issue_date
        )

        for cost_report_idx, aggregated_cost_report in enumerate(
            aggregated_cost_report_results, start=start_cost_report_number
        ):
            aggregated_cost_report["report_number"] = self.generate_report_number(
                report_month, issue_day, cost_report_idx
            )

            aggregated_cost_report["currency_date"] = (
                CostReportManager.get_currency_date(self.currency_date)
            )

            cost_report_vo = self.cost_report_mgr.create_cost_report(
                aggregated_cost_report
            )
            cost_report_data_svc.create_cost_report_data(cost_report_vo)
            if cost_report_vo.status == "SUCCESS":
                self.send_cost_report(cost_report_vo)

        _LOGGER.debug(
            f"[aggregate_monthly_cost_report] create cost report ({report_month}) \
            (count = {len(aggregated_cost_report_results)})"
        )

    def _get_all_cost_report_configs(self) -> QuerySet:
        return self.cost_report_config_mgr.filter_cost_report_configs(state="ENABLED")

    def _delete_old_cost_reports(
        self,
        report_month: str,
        domain_id: str,
        cost_report_config_id: str,
        status: str,
        cost_report_created_at: datetime,
    ) -> None:
        cost_report_delete_query = {
            "filter": [
                {"k": "cost_report_config_id", "v": cost_report_config_id, "o": "eq"},
                {"k": "report_month", "v": report_month, "o": "eq"},
                {"k": "status", "v": status, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
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
            cost_report_vo.delete()

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
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")

        if "WORKSPACE_OWNER" not in role_types:
            return []

        # list users in workspace
        users_info = identity_mgr.list_workspace_users(
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

    def _get_console_cost_report_url(
        self, domain_id: str, cost_report_id: str, token: str, language: str
    ) -> str:
        domain_name = self._get_domain_name(domain_id)

        console_domain = config.get_global("EMAIL_CONSOLE_DOMAIN")
        console_domain = console_domain.format(domain_name=domain_name)

        return f"{console_domain}/cost-report-detail?sso_access_token={token}&cost_report_id={cost_report_id}&language={language}"

    def _get_domain_name(self, domain_id: str) -> str:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        domain_name = identity_mgr.get_domain_name(domain_id)
        return domain_name

    def _get_temporary_sso_access_token(self, domain_id: str, workspace_id: str) -> str:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
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
        return identity_mgr.grant_token(params)

    def _get_virtual_workspace_ids_and_map(
        self, domain_id: str, workspace_ids: list
    ) -> Tuple[list, dict]:
        v_workspace_ids = []
        v_workspace_id_map = {}

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
            ]
        }
        ds_account_vos, _ = self.ds_account_mgr.list_data_source_accounts(query)

        for ds_account_vo in ds_account_vos:
            v_workspace_ids.append(ds_account_vo.v_workspace_id)
            if not v_workspace_id_map.get(ds_account_vo.v_workspace_id):
                v_workspace_id_map[ds_account_vo.v_workspace_id] = (
                    ds_account_vo.workspace_id
                )

        return v_workspace_ids, v_workspace_id_map

    def _get_data_source_currency_map(
        self, domain_id: str, workspace_ids: list, data_source_filter: dict
    ) -> Tuple[dict, list]:
        data_source_currency_map = {}
        data_source_mgr = DataSourceManager()

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids + ["*"], "o": "in"},
            ]
        }
        if data_sources := data_source_filter.get("data_sources"):
            query["filter"].append(
                {"k": "data_source_id", "v": data_sources, "o": "in"}
            )

        if data_source_state := data_source_filter.get("state", "ENABLED"):
            query["filter"].append({"k": "state", "v": data_source_state, "o": "eq"})

        _LOGGER.debug(f"[get_data_source_currency_map] query: {query}")

        data_source_vos, total_count = data_source_mgr.list_data_sources(query)
        data_source_ids = []
        for data_source_vo in data_source_vos:
            currency = self._get_currency_from_plugin_info(data_source_vo.plugin_info)
            data_source_currency_map[data_source_vo.data_source_id] = currency
            data_source_ids.append(data_source_vo.data_source_id)

        return data_source_currency_map, data_source_ids

    def _get_is_create_report_and_report_month(
        self,
        issue_day: int,
        is_last_day: bool,
        current_date: datetime,
        domain_id: str,
        cost_report_config_id: str,
    ) -> Tuple[bool, str]:
        is_create_report = False

        retry_days = min(config.get_global("COST_REPORT_RETRY_DAYS", 7), 25)

        current_day = current_date.day
        retry_date = current_date - relativedelta(days=retry_days)

        if retry_date.month != current_date.month:
            issue_date = (current_date - relativedelta(months=1)).replace(day=issue_day)

            report_date = current_date - relativedelta(months=2)
            issue_day = self.get_issue_day(is_last_day, issue_day)
        else:
            issue_date = current_date.replace(day=issue_day)
            report_date = current_date - relativedelta(months=1)

        if (
            current_date < report_date
            and retry_date <= issue_date.replace(day=issue_day) <= current_date
        ):
            is_create_report = True

        report_month = report_date.strftime("%Y-%m")

        if is_create_report and self._check_success_cost_report_exist(
            domain_id, cost_report_config_id, report_month
        ):
            is_create_report = False

        if issue_day == current_day:
            is_create_report = True
            report_month = (current_date - relativedelta(months=1)).strftime("%Y-%m")

        return is_create_report, report_month

    def _check_success_cost_report_exist(
        self,
        domain_id: str,
        cost_report_config_id: str,
        report_month: str,
    ):
        cost_report_vos = self.cost_report_mgr.filter_cost_reports(
            cost_report_config_id=cost_report_config_id,
            status="SUCCESS",
            domain_id=domain_id,
            report_month=report_month,
        )

        if cost_report_vos.count() > 0:
            return True
        else:
            return False

    @staticmethod
    def get_issue_day(is_last_day: bool, issue_day: int = None) -> int:
        current_date = datetime.utcnow()
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

    @staticmethod
    def _get_currency_from_plugin_info(plugin_info: dict) -> str:
        currency = plugin_info["metadata"].get("currency", "USD")

        metadata_cost_info = plugin_info["metadata"].get("cost_info", {})
        if metadata_cost_info:
            currency = metadata_cost_info.get("unit", "USD")

        return currency

    @staticmethod
    def _aggregate_result_by_currency(results: list) -> list:
        workspace_result_map = {}
        for result in results:
            workspace_id = result["workspace_id"]
            if workspace_id in workspace_result_map:
                for currency, cost in result["cost"].items():
                    workspace_result_map[workspace_id]["cost"][currency] += cost
            else:
                workspace_result_map[workspace_id] = result.copy()

        return [workspace_result for workspace_result in workspace_result_map.values()]

    @staticmethod
    def create_default_cost_report_config_for_all_domains():
        identity_mgr = IdentityManager()
        cost_report_config_mgr = CostReportConfigManager()

        params = {
            "query": {
                "filter": [{"k": "state", "v": "ENABLED", "o": "eq"}],
                "only": ["domain_id", "state"],
            }
        }

        response = identity_mgr.list_domains(params)
        domain_infos = response.get("results", [])

        for domain_info in domain_infos:
            domain_id = domain_info["domain_id"]
            cost_report_config_mgr.create_default_cost_report_config(domain_id)

    @staticmethod
    def _get_issue_month_fom_report_month(report_month: str) -> str:
        """
        report_month: 2024-06
        """
        issue_month_datetime = datetime.strptime(report_month, "%Y-%m") + relativedelta(
            months=1
        )
        issue_month = issue_month_datetime.strftime("%Y-%m")
        return issue_month

    @staticmethod
    def _get_currency_date_from_report_month(report_month: str, issue_day: int):
        report_month = datetime.strptime(report_month, "%Y-%m").replace(
            day=issue_day
        ) + relativedelta(months=1)
        return report_month
