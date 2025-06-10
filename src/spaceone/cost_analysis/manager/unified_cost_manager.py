import logging
from datetime import datetime
from typing import Tuple, Union

from dateutil.relativedelta import relativedelta
from mongoengine import QuerySet

from spaceone.core import queue, utils, config, cache
from spaceone.core.error import *
from spaceone.core.manager import BaseManager

from spaceone.cost_analysis.error import ERROR_INVALID_DATE_RANGE
from spaceone.cost_analysis.manager import DataSourceAccountManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.unified_cost.database import UnifiedCost

_LOGGER = logging.getLogger(__name__)


class UnifiedCostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unified_cost_model = UnifiedCost
        self.ds_account_mgr = DataSourceAccountManager()

    def create_unified_cost(self, params: dict) -> UnifiedCost:
        def _rollback(vo: UnifiedCost):
            _LOGGER.info(
                f"[create_unified_cost._rollback] Delete unified_cost : {vo.unified_cost_id}, {vo.to_dict()} "
            )
            vo.delete()

        unified_cost_vo: UnifiedCost = self.unified_cost_model.create(params)
        self.transaction.add_rollback(_rollback, unified_cost_vo)

        return unified_cost_vo

    @staticmethod
    def delete_unified_cost_by_vo(unified_cost_vo: UnifiedCost) -> None:
        unified_cost_vo.delete()

    def get_unified_cost(
        self,
        unified_cost_id: str,
        domain_id: str,
        workspace_id: str = None,
        project_id: Union[list, str] = None,
    ) -> UnifiedCost:
        conditions = {
            "unified_cost_id": unified_cost_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if project_id:
            conditions["project_id"] = project_id

        return self.unified_cost_model.get(**conditions)

    def filter_unified_costs(self, **conditions) -> QuerySet:
        return self.unified_cost_model.filter(**conditions)

    def list_unified_costs(self, query: dict) -> Tuple[QuerySet, int]:
        return self.unified_cost_model.query(**query)

    def analyze_unified_costs(
        self, query: dict, domain_id: str, target="SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "billed_month"
        query["date_field_format"] = "%Y-%m"

        query = self._change_filter_project_group_id(query, domain_id)

        _LOGGER.debug(f"[analyze_unified_costs] query: {query}")

        return self.unified_cost_model.analyze(**query)

    def analyze_yearly_unified_costs(
        self, query, domain_id, target="SECONDARY_PREFERRED"
    ):
        query["target"] = target
        query["date_field"] = "billed_year"
        query["date_field_format"] = "%Y"

        query = self._change_filter_project_group_id(query, domain_id)

        _LOGGER.debug(f"[analyze_unified_costs] query: {query}")

        return self.unified_cost_model.analyze(**query)

    @cache.cacheable(
        key="cost-analysis:analyze-unified-costs:yearly:{domain_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_unified_yearly_costs_with_cache(
        self, query: dict, query_hash: str, domain_id: str
    ) -> dict:
        return self.analyze_unified_costs(query, domain_id)

    @cache.cacheable(
        key="cost-analysis:analyze-unified-costs:monthly:{domain_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_unified_monthly_costs_with_cache(
        self, query: dict, query_hash: str, domain_id: str
    ) -> dict:
        return self.analyze_unified_costs(query, domain_id)

    def analyze_unified_costs_by_granularity(self, query: dict, domain_id: str) -> dict:

        self._check_unified_cost_data_range(query)
        granularity = query["granularity"]

        # Save query history to speed up data loading
        query_hash: str = utils.dict_to_hash(query)

        if granularity == "DAILY":
            raise ERROR_INVALID_PARAMETER(
                key="query.granularity", reason=f"{granularity} is not supported"
            )
        elif granularity == "MONTHLY":
            response = self.analyze_unified_monthly_costs_with_cache(
                query, query_hash, domain_id
            )
        else:
            response = self.analyze_unified_yearly_costs_with_cache(
                query, query_hash, domain_id
            )

        return response

    def analyze_unified_cost_for_report(
        self,
        report_month: str,
        data_source_ids: list,
        domain_id: str,
        workspace_ids: list,
        scope: str,
    ) -> list:
        currencies = config.get_global(
            "SUPPORTED_CURRENCIES", default=["USD", "KRW", "JPY"]
        )

        default_group_by = [
            "workspace_id",
            "billed_year",
            "billed_month",
            "exchange_date",
            "usage_type",
            "usage_unit",
            "region_key",
            "region_code",
        ]

        if scope == "WORKSPACE":
            default_group_by.append("workspace_name")
        elif scope == "PROJECT":
            default_group_by.append("project_id")
            default_group_by.append("project_name")
        elif scope == "SERVICE_ACCOUNT":
            default_group_by.append("service_account_id")
            default_group_by.append("service_account_name")

        # collect enabled data_sources cost data
        query = {
            "group_by": default_group_by,
            "start": report_month,
            "end": report_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_ids, "o": "in"},
                {"k": "billed_year", "v": report_month.split("-")[0], "o": "eq"},
                {"k": "billed_month", "v": report_month, "o": "eq"},
                {"k": "data_source_id", "v": data_source_ids, "o": "in"},
            ],
        }

        fields = {
            f"cost_{currency}": {"key": f"cost.{currency}", "operator": "sum"}
            for currency in currencies
        }
        query["fields"] = fields

        _LOGGER.debug(f"[aggregate_monthly_cost_report] query: {query}")
        response = self.analyze_unified_costs(query, domain_id)
        return response.get("results", [])

    def stat_unified_costs(self, query) -> dict:
        return self.unified_cost_model.stat(**query)

    @staticmethod
    def remove_stat_cache(domain_id: str):
        cache.delete_pattern(f"cost-analysis:analyze-unified-costs:*:{domain_id}:*")
        cache.delete_pattern(f"cost-analysis:stat-unified-costs:*:{domain_id}:*")

        _LOGGER.debug(f"[remove_stat_cache] domain_id: {domain_id}")

    @staticmethod
    def push_unified_cost_job_task(params: dict) -> None:
        token = config.get_global("TOKEN")
        task = {
            "name": "run_unified_cost",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                {
                    "locator": "SERVICE",
                    "name": "UnifiedCostService",
                    "metadata": {"token": token},
                    "method": "run_unified_cost",
                    "params": {"params": params},
                }
            ],
        }

        _LOGGER.debug(f"[push_job_task] task param: {params}")

        queue.put("cost_analysis_q", utils.dump_json(task))

    @staticmethod
    def get_exchange_currency(cost: float, currency: str, currency_map: dict) -> dict:
        cost_info = {}
        for convert_currency in currency_map.keys():
            cost_info.update(
                {
                    convert_currency: currency_map[currency][
                        f"{currency}/{convert_currency}"
                    ]
                    * cost
                }
            )

        return cost_info

    def _check_unified_cost_data_range(self, query: dict):
        start_str = query.get("start")
        end_str = query.get("end")
        granularity = query.get("granularity")

        start = self._parse_start_time(start_str, granularity)
        end = self._parse_end_time(end_str, granularity)
        now = datetime.utcnow().date()

        if len(start_str) != len(end_str):
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="Start date and end date must be the same format.",
            )

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="End date must be greater than start date.",
            )

        if granularity == "MONTHLY":
            if start + relativedelta(months=24) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 12 months.",
                )

            if start + relativedelta(months=48) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For MONTHLY, you cannot request data older than 3 years.",
                )
        elif granularity == "YEARLY":
            if start + relativedelta(years=5) < now.replace(month=1, day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For YEARLY, you cannot request data older than 3 years.",
                )

    @staticmethod
    def _convert_date_from_string(date_str, key, granularity):
        if granularity == "YEARLY":
            date_format = "%Y"
            date_type = "YYYY"
        else:
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            else:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_type)

    def _parse_start_time(self, date_str, granularity):
        return self._convert_date_from_string(date_str.strip(), "start", granularity)

    def _parse_end_time(self, date_str, granularity):
        end = self._convert_date_from_string(date_str.strip(), "end", granularity)

        if granularity == "YEARLY":
            return end + relativedelta(years=1)
        else:
            return end + relativedelta(months=1)

    def _change_filter_project_group_id(self, query: dict, domain_id: str) -> dict:
        change_filter = []
        self.identity_mgr = None

        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key == "project_group_id":
                if self.identity_mgr is None:
                    self.identity_mgr: IdentityManager = self.locator.get_manager(
                        "IdentityManager"
                    )

                project_groups_info = self.identity_mgr.list_project_groups(
                    {
                        "query": {
                            "only": ["project_group_id"],
                            "filter": [{"k": key, "v": value, "o": operator}],
                        }
                    },
                    domain_id,
                )

                project_group_ids = [
                    project_group_info["project_group_id"]
                    for project_group_info in project_groups_info.get("results", [])
                ]

                project_ids = []

                for project_group_id in project_group_ids:
                    projects_info = self.identity_mgr.get_projects_in_project_group(
                        project_group_id, domain_id
                    )
                    project_ids.extend(
                        [
                            project_info["project_id"]
                            for project_info in projects_info.get("results", [])
                        ]
                    )

                project_ids = list(set(project_ids))
                change_filter.append({"k": "project_id", "v": project_ids, "o": "in"})

            else:
                change_filter.append(condition)

        query["filter"] = change_filter
        return query
