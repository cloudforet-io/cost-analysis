import calendar
import copy
import logging
from typing import Union, Tuple
from datetime import datetime

from dateutil.relativedelta import relativedelta
from spaceone.core import config
from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager import DataSourceAccountManager
from spaceone.cost_analysis.manager.config_manager import ConfigManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.unified_cost_job_manager import (
    UnifiedCostJobManager,
)
from spaceone.cost_analysis.manager.unified_cost_manager import UnifiedCostManager
from spaceone.cost_analysis.model.unified_cost.database import UnifiedCostJob
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
        self.cost_mgr = CostManager()
        self.data_source_mgr = DataSourceManager()
        self.ds_account_mgr = DataSourceAccountManager()
        self.unified_cost_mgr = UnifiedCostManager()
        self.unified_cost_job_mgr = UnifiedCostJobManager()

    @transaction(exclude=["authenticate", "authorization", "mutation"])
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
        current_date = datetime.utcnow()
        current_month = current_date.strftime("%Y-%m")

        identity_mgr = IdentityManager()
        domain_ids = identity_mgr.list_enabled_domain_ids()

        for domain_id in domain_ids:
            try:
                unified_cost_config = config_mgr.get_unified_cost_config(domain_id)
                unified_cost_run_hour = unified_cost_config.get("run_hour")

                if current_hour == unified_cost_run_hour:
                    self.run_current_month_unified_costs(domain_id)

                    if not self._check_unified_cost_job_is_confirmed_with_month(
                        domain_id, current_month
                    ):
                        last_month = (current_date - relativedelta(months=1)).strftime(
                            "%Y-%m"
                        )
                        self.run_last_month_unified_costs(domain_id, last_month)

            except Exception as e:
                _LOGGER.error(
                    f"[create_unified_cost_jobs_by_domain_config] domain_id :{domain_id}, error: {e}",
                    exc_info=True,
                )

    @transaction(exclude=["authenticate", "authorization", "mutation"])
    def run_unified_cost(self, params: dict):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                "month": 'str', (optional),
            }
        """

        domain_id = params["domain_id"]
        aggregation_month: Union[str, None] = params.get("month")

        config_mgr = ConfigManager()
        unified_cost_config = config_mgr.get_unified_cost_config(domain_id)

        if not aggregation_month:
            aggregation_month = datetime.utcnow().strftime("%Y-%m")
            is_confirmed = False
        else:
            is_confirmed = self._get_is_confirmed_with_aggregation_month(
                aggregation_month, unified_cost_config
            )

        unified_cost_job_vo = self._get_unified_cost_job(domain_id, aggregation_month)

        aggregation_execution_date = self._get_aggregation_date(
            unified_cost_config, aggregation_month, is_confirmed
        )
        exchange_date = self._get_exchange_date(
            unified_cost_config, aggregation_month, is_confirmed
        )

        # exchange_rate_mode = unified_cost_config.get("exchange_rate_mode", "AUTO")
        exchange_rate_mode = "AUTO"
        if exchange_rate_mode == "AUTO":
            currency_mgr = CurrencyManager()
            currency_map, exchange_date = currency_mgr.get_currency_map_date(
                currency_end_date=exchange_date
            )
            exchange_source = "Yahoo! Finance"
        else:
            currency_map = unified_cost_config["custom_exchange_rate"]
            exchange_source = unified_cost_config.get("exchange_source", "MANUAL")

        workspace_ids = self._get_workspace_ids_with_none(domain_id)

        try:

            for workspace_id in workspace_ids:
                unified_cost_created_at = datetime.utcnow()
                self.create_unified_cost_with_workspace(
                    exchange_source,
                    domain_id,
                    workspace_id,
                    currency_map,
                    exchange_date,
                    aggregation_execution_date,
                    aggregation_month,
                    is_confirmed,
                )
                self._delete_old_unified_costs(
                    domain_id,
                    workspace_id,
                    aggregation_month,
                    is_confirmed,
                    unified_cost_created_at,
                )

            self.unified_cost_job_mgr.update_is_confirmed_unified_cost_job(
                unified_cost_job_vo, is_confirmed
            )
        except Exception as e:
            _LOGGER.error(f"[run_unified_cost] error: {e}", exc_info=True)
            self.unified_cost_job_mgr.update_is_confirmed_unified_cost_job(
                unified_cost_job_vo, False
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

        unified_cost_vo = self.unified_cost_mgr.get_unified_cost(
            unified_cost_id=params.unified_cost_id,
            domain_id=params.domain_id,
            workspace_id=params.workspace_id,
            project_id=params.users_projects,
        )

        return UnifiedCostResponse(**unified_cost_vo.to_dict())

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

        return self.unified_cost_mgr.analyze_unified_costs_by_granularity(query)

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
        workspace_id: Union[str, None],
        currency_map: dict,
        exchange_date: datetime,
        aggregation_execution_date: datetime,
        aggregation_month: str,
        is_confirmed: bool = False,
    ) -> None:

        identity_mgr = IdentityManager(token=config.get_global("TOKEN"))
        workspace_ids = [workspace_id]
        workspace_name = None
        project_name_map = {}
        service_account_name_map = {}

        if workspace_id:
            workspace_name = identity_mgr.get_workspace(workspace_id, domain_id)
            v_workspace_ids = self._get_virtual_workspace_ids_from_ds_account(
                domain_id, workspace_id
            )
            if v_workspace_ids:
                workspace_ids.extend(v_workspace_ids)

            project_name_map = identity_mgr.get_project_name_map(
                domain_id, workspace_id
            )

            service_account_name_map = identity_mgr.get_service_account_name_map(
                domain_id, workspace_id
            )

        data_source_currency_map, data_source_name_map, data_source_ids = (
            self._get_data_source_currency_map(domain_id, workspace_id)
        )

        unified_cost_billed_year = aggregation_month.split("-")[0]

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
            "start": aggregation_month,
            "end": aggregation_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "data_source_id", "v": data_source_ids, "o": "in"},
                {"k": "billed_month", "v": aggregation_month, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                {"k": "billed_year", "v": unified_cost_billed_year, "o": "eq"},
            ],
            "return_type": "cursor",
        }

        _LOGGER.debug(
            f"[create_unified_cost_with_workspace] monthly_costs query: {query}"
        )

        # todo : use cursor
        cursor = self.cost_mgr.analyze_monthly_costs(query, domain_id)

        exchange_date_str = exchange_date.strftime("%Y-%m-%d")
        aggregation_execution_date_str = aggregation_execution_date.strftime("%Y-%m-%d")

        row_count = 0
        for row in cursor:
            aggregated_unified_cost_data = copy.deepcopy(row)

            for key, value in row.get("_id", {}).items():
                aggregated_unified_cost_data[key] = value

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
            _single_cost = aggregated_unified_cost_data.get("cost", 0)
            aggregated_unified_cost_data["cost"] = (
                self.unified_cost_mgr.get_exchange_currency(
                    _single_cost, unified_cost_origin_currency, currency_map
                )
            )

            # set domain id
            aggregated_unified_cost_data["domain_id"] = domain_id

            # set workspace name
            aggregated_unified_cost_data["workspace_id"] = workspace_id
            if workspace_id:
                aggregated_unified_cost_data["workspace_name"] = workspace_name
                # set project name
                project_id = aggregated_unified_cost_data.get("project_id", None)
                aggregated_unified_cost_data["project_name"] = project_name_map.get(
                    project_id, project_id
                )

                # set service account name
                service_account_id = aggregated_unified_cost_data.get(
                    "service_account_id"
                )
                aggregated_unified_cost_data["service_account_name"] = (
                    service_account_name_map.get(service_account_id)
                )

            aggregated_unified_cost_data["exchange_date"] = exchange_date_str
            aggregated_unified_cost_data["exchange_source"] = exchange_source

            aggregated_unified_cost_data["is_confirmed"] = is_confirmed
            aggregated_unified_cost_data["aggregation_date"] = (
                aggregation_execution_date_str
            )

            self.unified_cost_mgr.create_unified_cost(aggregated_unified_cost_data)
            row_count += 1

        _LOGGER.debug(
            f"[create_unified_cost_with_workspace] create count: {row_count} (workspace_id: {workspace_id})"
        )

    def _get_data_source_currency_map(
        self, domain_id: str, workspace_id: Union[str, None]
    ) -> Tuple[dict, dict, list]:
        data_source_currency_map = {}
        data_source_name_map = {}
        workspace_ids = ["*"]

        if workspace_id:
            workspace_ids.append(workspace_id)

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
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
        self, domain_id: str, workspace_id: Union[str, None]
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

        query_filter = {
            "filter": [
                {"key": "workspace_id", "value": workspace_id, "operator": "eq"},
                {
                    "key": "billed_year",
                    "value": unified_cost_month.split("-")[0],
                    "operator": "eq",
                },
                {"key": "billed_month", "value": unified_cost_month, "operator": "eq"},
                {"key": "domain_id", "value": domain_id, "operator": "eq"},
                {"key": "created_at", "value": created_at, "operator": "lt"},
            ],
        }

        _LOGGER.debug(
            f"[delete_old_unified_costs] delete query filter conditions: {query_filter}"
        )

        unified_cost_vos, total_count = self.unified_cost_mgr.list_unified_costs(
            query=query_filter
        )

        _LOGGER.debug(
            f"[delete_old_unified_costs] delete count: {total_count} ({unified_cost_month})({workspace_id}, is_confirmed: {is_confirmed})"
        )
        unified_cost_vos.delete()

    def _check_unified_cost_job_is_confirmed_with_month(
        self, domain_id: str, current_month: str
    ) -> bool:
        unified_cost_job_vos = self.unified_cost_job_mgr.filter_unified_cost_jobs(
            domain_id=domain_id, billed_month=current_month
        )
        if unified_cost_job_vos:
            unified_cost_job_vo = unified_cost_job_vos[0]

            return unified_cost_job_vo.is_confirmed
        else:
            return False

    def _get_unified_cost_job(
        self, domain_id: str, aggregation_month: str
    ) -> UnifiedCostJob:

        unified_cost_job_vos = self.unified_cost_job_mgr.filter_unified_cost_jobs(
            domain_id=domain_id, billed_month=aggregation_month
        )
        if not unified_cost_job_vos:
            unified_cost_job_vo = self.unified_cost_job_mgr.create_unified_cost(
                {
                    "domain_id": domain_id,
                    "billed_month": aggregation_month,
                }
            )
        else:
            unified_cost_job_vo = unified_cost_job_vos[0]
        return unified_cost_job_vo

    def _get_aggregation_date(
        self,
        unified_cost_config: dict,
        aggregation_month: str,
        is_confirmed: bool = False,
    ) -> datetime:
        if is_confirmed:
            is_last_day = unified_cost_config.get("is_last_day", False)
            aggregation_day = unified_cost_config.get("aggregation_day", 15)

            aggregation_date: datetime = datetime.strptime(aggregation_month, "%Y-%m")
            aggregation_day = self.get_is_last_day(
                aggregation_date, is_last_day, aggregation_day
            )
            aggregation_date = aggregation_date.replace(day=aggregation_day)
        else:
            aggregation_date = datetime.utcnow()

        return aggregation_date

    def _get_exchange_date(
        self,
        unified_cost_config: dict,
        aggregation_month: str,
        is_confirmed: bool = False,
    ) -> datetime:
        if is_confirmed:
            is_exchange_last_day = unified_cost_config.get(
                "is_exchange_last_day", False
            )
            exchange_day = unified_cost_config.get("exchange_day", 15)

            exchange_date: datetime = datetime.strptime(aggregation_month, "%Y-%m")
            exchange_day = self.get_is_last_day(
                exchange_date, is_exchange_last_day, exchange_day
            )
            exchange_date = exchange_date.replace(day=exchange_day)
        else:
            exchange_date = datetime.utcnow()

        return exchange_date

    @staticmethod
    def get_is_last_day(
        current_date: datetime, is_last_day: bool, current_day: int
    ) -> int:
        current_year = current_date.year
        current_month = current_date.month

        _, last_day = calendar.monthrange(current_year, current_month)

        if is_last_day:
            return int(last_day)
        else:
            return int(min(current_day, last_day))

    @staticmethod
    def _get_workspace_ids_with_none(domain_id: str) -> list:
        workspace_ids = [None]

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

    def _get_is_confirmed_with_aggregation_month(
        self, aggregation_month: str, unified_cost_config: dict
    ) -> bool:
        is_confirmed = False
        current_date: datetime = datetime.utcnow()
        aggregation_date = datetime.strptime(aggregation_month, "%Y-%m")

        aggregation_day = unified_cost_config.get("aggregation_day", 15)
        is_last_day = unified_cost_config.get("is_last_day", False)

        last_day = self.get_is_last_day(current_date, is_last_day, aggregation_day)

        aggregation_date = aggregation_date.replace(day=last_day) + relativedelta(
            months=1
        )
        if current_date >= aggregation_date:
            is_confirmed = True

        return is_confirmed
