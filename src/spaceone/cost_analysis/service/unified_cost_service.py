import calendar
import logging
from typing import Union, Tuple
from datetime import datetime

from spaceone.core import config
from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager import DataSourceAccountManager
from spaceone.cost_analysis.manager.config_manager import ConfigManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.unified_cost_manager import UnifiedCostManager
from spaceone.cost_analysis.model.unified_cost.request import *
from spaceone.cost_analysis.model.unified_cost.response import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class UnifiedCostService(BaseService):
    resource = "UnifiedCost"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr = DataSourceManager()
        self.cost_mgr = CostManager()
        self.ds_account_mgr = DataSourceAccountManager()
        self.unified_cost_mgr = UnifiedCostManager()
        self.currency_date: Union[str, None] = None

    def run_unified_cost_by_scheduler(self, params: dict) -> None:
        """Create cost report by cost report config
        Args:
            params (dict): {
                'current_hour': 'int'
            }
        Returns:
            None
        """

        config_mgr = ConfigManager()
        current_hour = params["current_hour"]
        current_month = datetime.utcnow().strftime("%Y-%m")

        list_domain_params = {
            "query": {
                "filter": [{"k": "state", "v": "ENABLED", "o": "eq"}],
                "only": ["domain_id", "state"],
            }
        }

        identity_mgr = IdentityManager()
        response = identity_mgr.list_domains(list_domain_params)

        for domain_info in response.get("results", []):
            try:
                domain_id = domain_info["domain_id"]

                unified_cost_config = config_mgr.get_unified_cost_config(domain_id)
                unified_cost_run_hour = unified_cost_config.get("run_hour")

                if current_hour == unified_cost_run_hour:
                    self.run_current_month_unified_costs(domain_id)
                    self.run_last_month_unified_costs(domain_id, current_month)

            except Exception as e:
                _LOGGER.error(
                    f"[create_unified_cost_jobs_by_domain_config] error: {e}",
                    exc_info=True,
                )

    def run_unified_cost(self, params: dict):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                "month": 'str', (optional)
            }
        """

        _LOGGER.debug(
            f"[run_unified_cost] start run unified cost with params: {params}"
        )

        domain_id = params["domain_id"]

        config_mgr = ConfigManager()
        unified_cost_config = config_mgr.get_unified_cost_config(domain_id)

        is_last_day = unified_cost_config.get("is_last_day", False)
        aggregation_day = unified_cost_config.get("aggregation_day", 15)
        exchange_rate_mode = unified_cost_config.get("exchange_rate_mode", "AUTO")
        current_date = datetime.utcnow()

        if aggregation_month := params.get("month"):
            is_confirmed = True
            aggregation_date = datetime.strptime(aggregation_month, "%Y-%m")
            aggregation_day = self.get_is_last_day(
                aggregation_date, is_last_day, aggregation_day
            )
            aggregation_date = aggregation_date.replace(day=aggregation_day)

            if not self._check_aggregation_date_validity(
                aggregation_date, current_date
            ):
                return None

            is_exchange_last_day = unified_cost_config.get(
                "is_exchange_last_day", False
            )
            exchange_day = unified_cost_config.get("exchange_date", aggregation_day)
            exchange_day = self.get_is_last_day(
                aggregation_date, is_exchange_last_day, exchange_day
            )
            exchange_date = aggregation_date.replace(day=exchange_day)
        else:
            is_confirmed = False
            aggregation_date = current_date
            exchange_date = current_date

        if exchange_rate_mode == "AUTO":
            currency_mgr = CurrencyManager()
            currency_map, exchange_date = currency_mgr.get_currency_map_date(
                currency_end_date=exchange_date
            )
            exchange_source = unified_cost_config.get(
                "exchange_source", "Yahoo Finance!"
            )
        else:
            currency_map = unified_cost_config["custom_exchange_rate"]
            exchange_source = unified_cost_config.get("exchange_source", "MANUAL")

        workspace_ids = self._get_workspace_ids(domain_id)
        for workspace_id in workspace_ids:
            self.create_unified_cost_with_workspace(
                exchange_source,
                domain_id,
                workspace_id,
                currency_map,
                exchange_date,
                aggregation_date,
                is_confirmed,
            )

    @transaction(
        permission="cost-analysis:UnifiedCost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def get(self, params: UnifiedCostGetRequest) -> Union[UnifiedCostResponse, dict]:
        """Get unified cost data
        Args:
            params (dict): {
                'unified_cost_id': 'str'    # required
                'users_projects': 'list',   # injected from auth
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth
            }
        Returns:
            UnifiedCostResponse
        """

        cost_report_data_vo = self.unified_cost_mgr.get_unified_cost(
            unified_cost_id=params.unified_cost_id,
            domain_id=params.domain_id,
            workspace_id=params.workspace_id,
            project_id=params.users_projects,
        )
        return cost_report_data_vo

    @transaction(
        permission="cost-analysis:UnifiedCost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "workspace_id",
            "domain_id",
            "users_projects",
            "unified_cost_id",
        ]
    )
    @append_keyword_filter(
        [
            "unified_cost_id",
        ]
    )
    @set_query_page_limit(1000)
    @convert_model
    def list(
        self, params: UnifiedCostSearchQueryRequest
    ) -> Union[UnifiedCostResponse, dict]:
        """List cost report data
        Args:
            params (dict): {
                'query': 'dict',
                'unified_cost_id': 'str',
                'user_projects': 'list'
                'workspace_id': 'str',
                'domain_id': 'str'
            }
        Returns:
            UnifiedCostResponse
        """

        query = params.query or {}
        (
            cost_report_data_vos,
            total_count,
        ) = self.unified_cost_mgr.list_unified_costs(query)

        cost_reports_data_info = [
            cost_report_data_vo.to_dict()
            for cost_report_data_vo in cost_report_data_vos
        ]
        return UnifiedCostResponse(
            results=cost_reports_data_info, total_count=total_count
        )

    @transaction(
        permission="cost-analysis:UnifiedCost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        [
            "is_confirmed",
            "workspace_id",
            "domain_id",
            "users_projects",
            "unified_cost_id",
        ]
    )
    @append_keyword_filter(
        ["provider", "product", "workspace_name", "project_name", "unified_cost_id"]
    )
    @set_query_page_limit(1000)
    @convert_model
    def analyze(self, params: UnifiedCostAnalyzeQueryRequest) -> dict:
        """Analyze cost report data
        Args:
            params (dict): {
                'query': 'dict',
                'is_confirmed': 'bool',
                'user_projects': 'list',
                'workspace_id': 'str',
                'domain_id': 'str'
            }
        Returns:
            dict
        """

        query = params.query or {}

        return self.unified_cost_mgr.analyze_unified_costs(query)

    @transaction(
        permission="cost-analysis:UnifiedCost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "unified_cost_id",
            "workspace_id",
            "domain_id",
            "users_projects",
        ]
    )
    @append_keyword_filter(["unified_cost_id"])
    @convert_model
    def stat(self, params: UnifiedCostStatQueryRequest) -> dict:
        """Analyze cost report data
        Args:
            params (dict): {
                'query': 'dict',
                'user_projects': 'list',
                'workspace_id': 'str',
                'domain_id': 'str'
            }
        Returns:
            dict
        """

        query = params.query or {}
        return self.unified_cost_mgr.stat_unified_costs(query)

    def run_current_month_unified_costs(self, domain_id: str) -> None:
        self.unified_cost_mgr.push_unified_cost_job_task({"domain_id": domain_id})

    def run_last_month_unified_costs(
        self,
        domain_id: str,
        month: str = None,
    ) -> None:

        self.unified_cost_mgr.push_unified_cost_job_task(
            {"domain_id": domain_id, "month": month}
        )

    def create_unified_cost_with_workspace(
        self,
        exchange_source: str,
        domain_id: str,
        workspace_id: str,
        currency_map: dict,
        exchange_date: datetime,
        aggregation_date: datetime,
        is_confirmed: bool = False,
    ):
        identity_mgr = IdentityManager(token=config.get_global("TOKEN"))
        workspace_name = identity_mgr.get_workspace(workspace_id, domain_id)
        workspace_ids = [workspace_id]

        v_workspace_ids = self._get_virtual_workspace_ids_from_ds_account(
            domain_id, workspace_id
        )
        if v_workspace_ids:
            workspace_ids.extend(v_workspace_ids)

        data_source_currency_map, data_source_name_map, data_source_ids = (
            self._get_data_source_currency_map(domain_id, workspace_id)
        )

        project_name_map = identity_mgr.get_project_name_map(domain_id, workspace_id)

        service_account_name_map = identity_mgr.get_service_account_name_map(
            domain_id, workspace_id
        )

        unified_cost_billed_year = aggregation_date.strftime("%Y")
        unified_cost_billed_month = aggregation_date.strftime("%Y-%m")

        query = {
            "group_by": [
                "billed_year",
                "workspace_id",
                "project_id",
                "service_account_id",
                "data_source_id",
                "product",
                "provider",
                "region_code",
                "region_key",
                "usage_type",
                "usage_unit",
                "billed_year",
                "billed_month",
            ],
            "fields": {
                "cost": {"key": "cost", "operator": "sum"},
            },
            "start": unified_cost_billed_year,
            "end": unified_cost_billed_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "data_source_id", "v": data_source_ids, "o": "in"},
                {"k": "billed_month", "v": unified_cost_billed_month, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                {"k": "billed_year", "v": unified_cost_billed_year, "o": "eq"},
            ],
        }

        _LOGGER.debug(
            f"[create_unified_cost_with_workspace] monthly_costs query: {query}"
        )

        response = self.cost_mgr.analyze_monthly_costs(query, domain_id)
        results = response.get("results", [])

        exchange_date_str = exchange_date.strftime("%Y-%m-%d")
        aggregation_date_str = aggregation_date.strftime("%Y-%m-%d")
        unified_cost_created_at = datetime.utcnow()

        for aggregated_unified_cost_data in results:

            # set data source name and currency
            data_source_id = aggregated_unified_cost_data["data_source_id"]
            unified_cost_origin_currency = data_source_currency_map.get(
                data_source_id, "USD"
            )
            aggregated_unified_cost_data["data_source_name"] = data_source_name_map.get(
                data_source_id, data_source_id
            )
            aggregated_unified_cost_data["currency"] = unified_cost_origin_currency

            # set cost
            _unified_cost = aggregated_unified_cost_data.get("cost", 0)
            aggregated_unified_cost_data["cost"] = (
                self.unified_cost_mgr.get_exchange_currency(
                    _unified_cost, unified_cost_origin_currency, currency_map
                )
            )

            # set domain id
            aggregated_unified_cost_data["domain_id"] = domain_id

            # set workspace name
            aggregated_unified_cost_data["workspace_id"] = workspace_id
            aggregated_unified_cost_data["workspace_name"] = workspace_name

            # set project name
            project_id = aggregated_unified_cost_data.get("project_id")
            aggregated_unified_cost_data["project_name"] = project_name_map.get(
                project_id, project_id
            )

            # set service account name
            service_account_id = aggregated_unified_cost_data.get("service_account_id")
            aggregated_unified_cost_data["service_account_name"] = (
                service_account_name_map.get(service_account_id)
            )

            aggregated_unified_cost_data["exchange_date"] = exchange_date_str
            aggregated_unified_cost_data["exchange_source"] = exchange_source

            aggregated_unified_cost_data["is_confirmed"] = is_confirmed
            aggregated_unified_cost_data["aggregation_day"] = aggregation_date_str

            self.unified_cost_mgr.create_unified_cost(aggregated_unified_cost_data)

        _LOGGER.debug(
            f"[create_unified_cost_with_workspace] create count: {len(results)} (workspace_id: {workspace_id})"
        )
        self._delete_old_unified_costs(
            domain_id,
            workspace_id,
            unified_cost_billed_month,
            is_confirmed,
            unified_cost_created_at,
        )

    def _get_data_source_currency_map(
        self, domain_id: str, workspace_id: str
    ) -> Tuple[dict, dict, list]:
        data_source_currency_map = {}
        data_source_name_map = {}
        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": [workspace_id, "*"], "o": "in"},
            ]
        }

        _LOGGER.debug(f"[get_data_source_currency_map] query: {query}")

        data_source_vos, _ = self.data_source_mgr.list_data_sources(query)
        data_source_ids = []
        for data_source_vo in data_source_vos:
            data_source_currency_map[data_source_vo.data_source_id] = (
                data_source_vo.plugin_info["metadata"]["currency"]
            )
            data_source_name_map[data_source_vo.data_source_id] = data_source_vo.name
            data_source_ids.append(data_source_vo.data_source_id)

        return data_source_currency_map, data_source_name_map, data_source_ids

    def _get_virtual_workspace_ids_from_ds_account(
        self, domain_id: str, workspace_id: str
    ) -> list:
        v_workspace_ids = []
        ds_account_vos = self.ds_account_mgr.filter_data_source_accounts(
            domain_id=domain_id, workspace_id=workspace_id
        )

        for ds_account_vo in ds_account_vos:
            v_workspace_ids.append(ds_account_vo.v_workspace_id)

        return v_workspace_ids

    def _delete_old_unified_costs(
        self,
        domain_id: str,
        workspace_id: str,
        unified_cost_month: str,
        is_confirmed: bool,
        created_at: datetime,
    ):
        created_at_operator = "eq" if is_confirmed else "lt"
        query_filter = {
            "filter": [
                {"key": "workspace_id", "value": workspace_id, "operator": "eq"},
                {
                    "key": "billed_year",
                    "value": unified_cost_month.split("-")[0],
                    "operator": "eq",
                },
                {"key": "billed_month", "value": unified_cost_month, "operator": "eq"},
                {"key": "is_confirmed", "value": is_confirmed, "operator": "eq"},
                {"key": "domain_id", "value": domain_id, "operator": "eq"},
                {
                    "key": "created_at",
                    "value": created_at,
                    "operator": created_at_operator,
                },
            ],
        }

        _LOGGER.debug(
            f"[delete_old_unified_costs] delete query filter conditions: {query_filter}"
        )

        unified_cost_vos, total_count = self.unified_cost_mgr.list_unified_costs(
            query=query_filter
        )

        _LOGGER.debug(
            f"[delete_old_unified_costs] delete count: {total_count} ({unified_cost_month})({workspace_id})"
        )
        unified_cost_vos.delete()

    @staticmethod
    def get_is_last_day(
        issue_date: datetime, is_last_day: bool, issue_day: int = None
    ) -> int:
        issue_year = issue_date.year
        issue_month = issue_date.month

        _, last_day = calendar.monthrange(issue_year, issue_month)

        if is_last_day:
            return last_day
        else:
            return min(issue_day, last_day)

    @staticmethod
    def _get_workspace_ids(domain_id: str) -> list:
        workspace_ids = []

        identity_mgr = IdentityManager()
        system_token = config.get_global("TOKEN")

        response = identity_mgr.list_workspaces(
            {"query": {"filter": [{"k": "state", "v": "ENABLED", "o": "eq"}]}},
            domain_id,
            token=system_token,
        )
        for workspace in response.get("results", []):
            workspace_ids.append(workspace["workspace_id"])

        return workspace_ids

    @staticmethod
    def _get_project_name_map(
        identity_mgr: IdentityManager, domain_id: str, workspace_id: str
    ) -> dict:
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

    @staticmethod
    def _get_service_account_name_map(
        identity_mgr: IdentityManager, workspace_id: str, domain_id: str
    ) -> dict:
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
    def _check_aggregation_date_validity(
        aggregation_date: datetime, current_date: datetime
    ) -> bool:

        is_aggregation_date_valid = True
        if aggregation_date > current_date:
            _LOGGER.debug(
                f"skip unified cost aggregation, {aggregation_date} is greater than {current_date}."
            )
            is_aggregation_date_valid = False

        elif aggregation_date.month == current_date.month:
            _LOGGER.debug(
                f"skip unified cost aggregation, {aggregation_date} should be previous month of {current_date}."
            )
            is_aggregation_date_valid = False

        return is_aggregation_date_valid
