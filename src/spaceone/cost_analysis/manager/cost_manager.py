import logging
import copy
from datetime import datetime
from dateutil.relativedelta import relativedelta

from spaceone.core import cache, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.cost_model import Cost, MonthlyCost, CostQueryHistory
from spaceone.cost_analysis.manager.data_source_rule_manager import (
    DataSourceRuleManager,
)
from spaceone.cost_analysis.manager.data_source_account_manager import (
    DataSourceAccountManager,
)
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_model: Cost = self.locator.get_model("Cost")
        self.monthly_cost_model: MonthlyCost = self.locator.get_model("MonthlyCost")
        self.cost_query_history_model: CostQueryHistory = self.locator.get_model(
            "CostQueryHistory"
        )
        self.data_source_rule_mgr: DataSourceRuleManager = self.locator.get_manager(
            "DataSourceRuleManager"
        )
        self.data_source_account_mgr: DataSourceAccountManager = (
            self.locator.get_manager("DataSourceAccountManager")
        )
        self.data_source_mgr: DataSourceManager = self.locator.get_manager(
            "DataSourceManager"
        )

    def create_cost(self, params: dict, execute_rollback=True):
        def _rollback(vo: Cost):
            _LOGGER.info(f"[create_cost._rollback] " f"Delete cost : {vo.cost_id} ")
            vo.delete()

        if "region_code" in params and "provider" in params:
            params["region_key"] = f'{params["provider"]}.{params["region_code"]}'

        billed_at = self._get_billed_at_from_billed_date(params["billed_date"])

        params["billed_year"] = billed_at.strftime("%Y")
        params["billed_month"] = billed_at.strftime("%Y-%m")

        (
            workspace_id,
            v_workspace_id,
        ) = self.data_source_account_mgr.get_workspace_id_from_account_id(
            params, params["domain_id"], params["data_source_id"]
        )

        if v_workspace_id:
            params["workspace_id"] = v_workspace_id

        params = self.data_source_rule_mgr.change_cost_data(params, workspace_id)

        cost_vo: Cost = self.cost_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cost_vo)

        return cost_vo

    def create_monthly_cost(self, params):
        return self.monthly_cost_model.create(params)

    def delete_cost(self, cost_id, domain_id):
        cost_vo: Cost = self.get_cost(cost_id, domain_id)
        self.delete_cost_by_vo(cost_vo)

    @staticmethod
    def delete_cost_by_vo(cost_vo: Cost):
        cost_vo.delete()

    def delete_cost_with_datasource(self, domain_id: str, data_source_id: str) -> None:
        _LOGGER.debug(f"[delete_cost_with_datasource] data_source_id: {data_source_id}")
        cost_vos = self.cost_model.filter(
            domain_id=domain_id, data_source_id=data_source_id
        )
        cost_vos.delete()

        monthly_cost_vos = self.monthly_cost_model.filter(
            domain_id=domain_id, data_source_id=data_source_id
        )
        monthly_cost_vos.delete()

        history_vos = self.cost_query_history_model.filter(
            domain_id=domain_id, data_source_id=data_source_id
        )
        history_vos.delete()

    def get_cost(
        self,
        cost_id: str,
        domain_id: str,
        workspace_id=None,
        user_projects: list = None,
    ):
        conditions = {"cost_id": cost_id, "domain_id": domain_id}

        if workspace_id:
            self._get_workspace_id_from_v_workspace_id(domain_id, workspace_id)
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.cost_model.get(**conditions)

    def filter_costs(self, **conditions):
        return self.cost_model.filter(**conditions)

    def list_costs(self, query: dict, domain_id: str, data_source_id: str):
        query = self.change_filter_v_workspace_id(query, domain_id, data_source_id)
        query = self._change_filter_project_group_id(query, domain_id)
        return self.cost_model.query(**query)

    def stat_costs(self, query: dict, domain_id: str, data_source_id: str):
        query = self._change_filter_project_group_id(query, domain_id)
        return self.cost_model.stat(**query)

    def filter_monthly_costs(self, **conditions):
        return self.monthly_cost_model.filter(**conditions)

    def list_monthly_costs(self, query: dict, domain_id: str):
        query = self._change_filter_project_group_id(query, domain_id)
        return self.monthly_cost_model.query(**query)

    def stat_monthly_costs(self, query: dict, domain_id: str):
        query = self._change_filter_project_group_id(query, domain_id)
        return self.monthly_cost_model.stat(**query)

    def analyze_costs(self, query, domain_id, target="SECONDARY_PREFERRED"):
        query["target"] = target
        query["date_field"] = "billed_date"
        query["date_field_format"] = "%Y-%m-%d"
        _LOGGER.debug(f"[analyze_costs] query: {query}")

        query = self._change_filter_project_group_id(query, domain_id)
        return self.cost_model.analyze(**query)

    def analyze_monthly_costs(self, query, domain_id, target="SECONDARY_PREFERRED"):
        query["target"] = target
        query["date_field"] = "billed_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_monthly_costs] query: {query}")

        query = self._change_filter_project_group_id(query, domain_id)
        response = self.monthly_cost_model.analyze(**query)
        return response

    def analyze_yearly_costs(self, query, domain_id, target="SECONDARY_PREFERRED"):
        query["target"] = target
        query["date_field"] = "billed_year"
        query["date_field_format"] = "%Y"
        _LOGGER.debug(f"[analyze_yearly_costs] query: {query}")

        query = self._change_filter_project_group_id(query, domain_id)
        return self.monthly_cost_model.analyze(**query)

    @cache.cacheable(
        key="cost-analysis:stat-costs:monthly:{domain_id}:{data_source_id}:{query_hash}",
        expire=3600 * 24,
    )
    def stat_monthly_costs_with_cache(
        self, query, query_hash, domain_id, data_source_id
    ):
        return self.stat_monthly_costs(query, domain_id)

    @cache.cacheable(
        key="cost-analysis:analyze-costs:daily:{domain_id}:{data_source_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_costs_with_cache(
        self, query, query_hash, domain_id, data_source_id, target="SECONDARY_PREFERRED"
    ):
        return self.analyze_costs(query, domain_id, target)

    @cache.cacheable(
        key="cost-analysis:analyze-costs:monthly:{domain_id}:{data_source_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_monthly_costs_with_cache(
        self, query, query_hash, domain_id, data_source_id, target="SECONDARY_PREFERRED"
    ):
        return self.analyze_monthly_costs(query, domain_id, target)

    @cache.cacheable(
        key="cost-analysis:analyze-costs:yearly:{domain_id}:{data_source_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_yearly_costs_with_cache(
        self, query, query_hash, domain_id, data_source_id, target="SECONDARY_PREFERRED"
    ):
        return self.analyze_yearly_costs(query, domain_id, target)

    def analyze_costs_by_granularity(
        self, query: dict, domain_id: str, data_source_id: str
    ):
        self._check_group_by(query)
        self._check_date_range(query)
        granularity = query["granularity"]

        query = self.change_filter_v_workspace_id(query, domain_id, data_source_id)

        # Save query history to speed up data loading
        query_hash: str = utils.dict_to_hash(query)
        self.create_cost_query_history(query, query_hash, domain_id, data_source_id)

        if granularity == "DAILY":
            response = self.analyze_costs_with_cache(
                query, query_hash, domain_id, data_source_id
            )
        elif granularity == "MONTHLY":
            response = self.analyze_monthly_costs_with_cache(
                query, query_hash, domain_id, data_source_id
            )
        else:
            response = self.analyze_yearly_costs_with_cache(
                query, query_hash, domain_id, data_source_id
            )

        # change workspace_id to v_workspace_id
        response = self._change_response_workspace_group_by(
            response, query, domain_id, data_source_id
        )

        return response

    @cache.cacheable(
        key="cost-analysis:cost-query-history:{domain_id}:{data_source_id}:{query_hash}",
        expire=600,
    )
    def create_cost_query_history(
        self, query: dict, query_hash: str, domain_id: str, data_source_id: str
    ):
        def _rollback(history_vo):
            _LOGGER.info(
                f"[create_cost_query_history._rollback] Delete cost query history: {query_hash}"
            )
            history_vo.delete()

        history_model: CostQueryHistory = self.locator.get_model("CostQueryHistory")

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_vo = history_model.create(
                {
                    "query_hash": query_hash,
                    "query_options": copy.deepcopy(query),
                    "data_source_id": data_source_id,
                    "domain_id": domain_id,
                }
            )

            self.transaction.add_rollback(_rollback, history_vo)
        else:
            history_vos[0].update({})

    def list_cost_query_history(self, query: dict):
        history_model: CostQueryHistory = self.locator.get_model("CostQueryHistory")
        return history_model.query(**query)

    @staticmethod
    def remove_stat_cache(domain_id: str, data_source_id: str):
        cache.delete_pattern(
            f"cost-analysis:analyze-costs:*:{domain_id}:{data_source_id}:*"
        )
        cache.delete_pattern(
            f"cost-analysis:stat-costs:*:{domain_id}:{data_source_id}:*"
        )
        cache.delete_pattern(
            f"cost-analysis:cost-query-history:{domain_id}:{data_source_id}:*"
        )

    def _check_date_range(self, query):
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

        if granularity == "DAILY":
            if start + relativedelta(months=1) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 1 month.",
                )

            if start + relativedelta(months=12) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For DAILY, you cannot request data older than 1 year.",
                )

        elif granularity == "MONTHLY":
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 12 months.",
                )

            if start + relativedelta(months=36) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For MONTHLY, you cannot request data older than 3 years.",
                )
        elif granularity == "YEARLY":
            if start + relativedelta(years=3) < now.replace(month=1, day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For YEARLY, you cannot request data older than 3 years.",
                )

    def _parse_start_time(self, date_str, granularity):
        return self._convert_date_from_string(date_str.strip(), "start", granularity)

    def _parse_end_time(self, date_str, granularity):
        end = self._convert_date_from_string(date_str.strip(), "end", granularity)

        if granularity == "YEARLY":
            return end + relativedelta(years=1)
        elif granularity == "MONTHLY":
            return end + relativedelta(months=1)
        else:
            return end + relativedelta(days=1)

    @staticmethod
    def _convert_date_from_string(date_str, key, granularity):
        if granularity == "YEARLY":
            date_format = "%Y"
            date_type = "YYYY"
        elif granularity == "MONTHLY":
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            else:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"
        else:
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            elif len(date_str) == 7:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"
            else:
                date_format = "%Y-%m-%d"
                date_type = "YYYY-MM-DD"

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_type)

    @staticmethod
    def _get_billed_at_from_billed_date(billed_date):
        date_format = "%Y-%m-%d"

        try:
            return datetime.strptime(billed_date, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="billed_date", type="YYYY-MM-DD")

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

    def change_filter_v_workspace_id(
        self, query: dict, domain_id: str, data_source_id: str
    ) -> dict:
        change_filter = []
        workspace_ids = []

        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)
        plugin_metadata = data_source_vo.plugin_info.metadata
        use_account_routing = plugin_metadata.get("use_account_routing", False)
        if not use_account_routing:
            return query

        _LOGGER.debug(
            f"[analyze_costs_by_granularity] add v_workspace_id filter: {data_source_id}"
        )

        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key == "workspace_id":
                ds_account_list_query = {
                    "filter": [
                        {"k": "domain_id", "v": domain_id, "o": "eq"},
                        {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                        {"k": key, "v": value, "o": operator},
                    ]
                }
                (
                    ds_accounts_vos,
                    total_count,
                ) = self.data_source_account_mgr.list_data_source_accounts(
                    ds_account_list_query
                )
                if isinstance(value, list):
                    workspace_ids.extend(value)
                else:
                    workspace_ids.append(value)

                if total_count > 0:
                    for ds_accounts_vo in ds_accounts_vos:
                        if ds_accounts_vo.workspace_id:
                            workspace_ids.append(ds_accounts_vo.v_workspace_id)
                        if ds_accounts_vo.workspace_id in workspace_ids:
                            workspace_ids.remove(ds_accounts_vo.workspace_id)

                change_filter.append(
                    {"k": "workspace_id", "v": workspace_ids, "o": "in"}
                )

            else:
                change_filter.append(condition)
            query["filter"] = change_filter
        return query

    def _change_response_workspace_group_by(
        self, response: dict, query: dict, domain_id: str, data_source_id: str
    ) -> dict:
        if query_group_by := query.get("group_by"):
            if self._check_group_by_key(query_group_by):
                results = response.get("results")
                v_workspace_ids = [result.get("workspace_id") for result in results]

                ds_account_list_query = {
                    "filter": [
                        {"k": "domain_id", "v": domain_id, "o": "eq"},
                        {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                        {"k": "v_workspace_id", "v": v_workspace_ids, "o": "in"},
                    ]
                }

                (
                    data_source_account_vos,
                    _,
                ) = self.data_source_account_mgr.list_data_source_accounts(
                    ds_account_list_query
                )
                workspace_id_map = {
                    ds_account.v_workspace_id: ds_account.workspace_id
                    for ds_account in data_source_account_vos
                }
                for result in results:
                    workspace_id = result.get("workspace_id")
                    if workspace_id in workspace_id_map:
                        result["workspace_id"] = workspace_id_map[workspace_id]
                response["results"] = results
        return response

    def _get_workspace_id_from_v_workspace_id(
        self, domain_id: str, v_workspace_id: str
    ) -> str:
        workspace_id = v_workspace_id
        ds_account_vos = self.data_source_account_mgr.filter_data_source_accounts(
            domain_id=domain_id, v_workspace_id=v_workspace_id
        )
        if ds_account_vos:
            workspace_id = ds_account_vos[0].workspace_id

        return workspace_id

    @staticmethod
    def _check_group_by_key(query_group_by: list):
        for group_by_info in query_group_by:
            if isinstance(group_by_info, dict):
                key = group_by_info.get("key")
            else:
                key = group_by_info
            if key == "workspace_id":
                return True
        return False

    @staticmethod
    def _get_data_source_id_from_filter(query: dict) -> str:
        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))

            if key == "data_source_id":
                return value

    @staticmethod
    def _check_group_by(query: dict) -> None:
        group_by = query.get("group_by", [])
        for grop_by_option in group_by:
            key = None

            if isinstance(grop_by_option, dict):
                key = grop_by_option.get("key")
                name = grop_by_option.get("name")
                if not (key and name):
                    raise ERROR_INVALID_PARAMETER(
                        key="group_by",
                        reason="group_by option requires a key and name.",
                    )
            elif isinstance(grop_by_option, str):
                key = grop_by_option

            if key is None or not isinstance(key, str):
                raise ERROR_INVALID_PARAMETER(
                    key="group_by", reason="Group by key is not exist or invalid type."
                )
            elif key.split(".")[0] == "data":
                raise ERROR_INVALID_PARAMETER(
                    key=key, reason="Data field is not allowed to group by."
                )
            elif key in ["cost", "usage_quantity"]:
                raise ERROR_INVALID_PARAMETER(
                    key=key,
                    reason=f"{key} is not allowed to group by.",
                )
