import logging
from typing import Union

from spaceone.core.service import *
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager
from spaceone.cost_analysis.model.budget_usage.request import BudgetUsageSearchQueryRequest, \
    BudgetUsageStatQueryRequest, BudgetUsageAnalyzeQueryRequest
from spaceone.cost_analysis.model.budget_usage.response import BudgetUsagesResponse

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BudgetUsageService(BaseService):
    resource = "BudgetUsage"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_usage_mgr: BudgetUsageManager = self.locator.get_manager(
            "BudgetUsageManager"
        )

    @transaction(
        permission="cost-analysis:BudgetUsage.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "budget_id",
            "data_source_id",
            "name",
            "date",
            "user_projects",
            "project_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["budget_id", "name"])
    @set_query_page_limit(1000)
    def list(self, params: BudgetUsageSearchQueryRequest) -> Union[BudgetUsagesResponse, dict]:
        """List budget_usages

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'budget_id': 'str',
                'data_source_id': 'str',
                'name': 'str',
                'date': 'str',
                'project_id': 'str',
                'user_projects': 'list',
                'workspace_id': str,                                # injected from auth (optional)
                'domain_id': 'str',                                 # injected from auth
            }

        Returns:
            budget_usage_vos (object)
            total_count
        """
        params_dict = params.dict(exclude_unset=True)

        query = params_dict.get("query", {})
        return self.budget_usage_mgr.list_budget_usages(query)

    @transaction(
        permission="cost-analysis:BudgetUsage.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["budget_id", "data_source_id", "workspace_id", "domain_id"])
    @append_keyword_filter(["budget_id", "name"])
    @set_query_page_limit(1000)
    def stat(self, params: BudgetUsageStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                "budget_id": "str",
                'data_source_id': 'str',
                'workspace_id': 'str',                                # injected from auth (optional)
                'domain_id': 'str'                                    # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        params_dict = params.dict(exclude_unset=True)

        query = self._set_user_project_or_project_group_filter(params_dict)
        return self.budget_usage_mgr.stat_budget_usages(query)

    @transaction(
        permission="cost-analysis:BudgetUsage.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "query.fields", "domain_id"])
    @append_query_filter(
        ["data_source_id", "budget_id", "user_projects", "workspace_id", "domain_id"]
    )
    @append_keyword_filter(["budget_id", "name"])
    @set_query_page_limit(1000)
    def analyze(self, params: BudgetUsageAnalyzeQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v2.TimeSeriesAnalyzeQuery)',
                'budget_id': 'str',
                'data_source_id': 'str',
                'user_projects': 'list',                                        # injected from auth
                'workspace_id': 'str',                                          # injected from auth (optional)
                'domain_id': 'str'                                              # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        params_dict = params.dict(exclude_unset=True)

        query = self._set_user_project_or_project_group_filter(params_dict)
        self._check_granularity(query.get("granularity"))

        return self.budget_usage_mgr.analyze_budget_usages(query)

    @staticmethod
    def _check_granularity(granularity):
        if granularity and granularity != "MONTHLY":
            raise ERROR_INVALID_PARAMETER(
                key="query.granularity", reason="Granularity is only MONTHLY."
            )

    @staticmethod
    def _set_user_project_or_project_group_filter(params):
        query = params.get("query", {})
        query["filter"] = query.get("filter", [])

        if "user_projects" in params:
            user_projects = params["user_projects"] + [None]
            query["filter"].append(
                {"k": "user_projects", "v": user_projects, "o": "in"}
            )

        return query
