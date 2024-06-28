import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.model.cost.response import CostsResponse

from spaceone.cost_analysis.manager.data_source_account_manager import (
    DataSourceAccountManager,
)

from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager import DataSourceManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model import DataSource
from spaceone.cost_analysis.model.cost.response import CostResponse
from spaceone.cost_analysis.model.cost_model import Cost

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostService(BaseService):
    resource = "Cost"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager("CostManager")
        self.data_source_mgr = DataSourceManager()

    @transaction(permission="cost-analysis:Cost.write", role_types=["WORKSPACE_OWNER"])
    @check_required(
        ["cost", "data_source_id", "billed_date", "project_id", "domain_id"]
    )
    def create(self, params):
        """Register cost

        Args:
            params (dict): {
                'cost': 'float',            #required
                'usage_quantity': 'float',
                'usage_unit': 'str',
                'provider': 'str',
                'region_code': 'str',
                'product': 'str',
                'usage_type': 'str',
                'resource': 'str',
                'tags': 'dict',
                'additional_info': 'dict',
                'service_account_id': 'str',
                'project_id': 'str',         # required
                'data_source_id': 'str',     # required
                'billed_date': 'str',        # required
                'workspace_id': 'str',       # injected from auth
                'domain_id': 'str'           # injected from auth
            }

        Returns:
            cost_vo (object)
        """

        # validation check (service_account_id / project_id / data_source_id)
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        identity_mgr.get_project(params["project_id"], params["domain_id"])

        # todo : only local type datasource can create

        cost_vo: Cost = self.cost_mgr.create_cost(params)

        self.cost_mgr.remove_stat_cache(params["domain_id"], params["data_source_id"])

        return cost_vo

    @transaction(permission="cost-analysis:Cost.write", role_types=["WORKSPACE_OWNER"])
    @check_required(["cost_id", "domain_id"])
    def delete(self, params: dict):
        """Deregister cost

        Args:
            params (dict): {
                'cost_id': 'str',           # injected from path
                'workspace_id' : str',      # injected from auth
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            None
        """

        cost_id = params["cost_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        if workspace_id:
            cost_vo: Cost = self.cost_mgr.get_cost(cost_id, domain_id)
            self._check_workspace_id_with_cost_vo(cost_vo, domain_id, workspace_id)
        else:
            cost_vo: Cost = self.cost_mgr.get_cost(cost_id, domain_id)

        self.cost_mgr.remove_stat_cache(
            domain_id=domain_id,
            data_source_id=cost_vo.data_source_id,
        )

        self.cost_mgr.delete_cost_by_vo(cost_vo)

    @transaction(
        permission="cost-analysis:Cost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["cost_id", "domain_id"])
    @convert_model
    def get(self, params: dict) -> Union[CostResponse, dict]:
        """Get cost

        Args:
            params (dict): {
                'cost_id': 'str',
                'user_projects': 'list'     # injected from auth(optional)
                'workspace_id': 'str',      # injected from auth(optional)
                'domain_id': 'str',         # injected from auth
            }

        Returns:
            cost_vo (object)
        """

        cost_id = params["cost_id"]
        user_projects = params.get("user_projects", [])
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        if workspace_id:
            cost_vo: Cost = self.cost_mgr.get_cost(cost_id, domain_id, user_projects)
            self._check_workspace_id_with_cost_vo(cost_vo, domain_id, workspace_id)
        else:
            cost_vo: Cost = self.cost_mgr.get_cost(
                cost_id, domain_id, workspace_id, user_projects
            )

        cost_info = cost_vo.to_dict()

        # Check fields permissions
        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            data_source_id = cost_vo.data_source_id
            data_source_vo = self.data_source_mgr.get_data_source(
                data_source_id, domain_id
            )
            cost_info = self._remove_deny_fields_with_data_source_vo(
                cost_info, data_source_vo
            )

        return CostResponse(**cost_info)

    @transaction(
        permission="cost-analysis:Cost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["data_source_id", "domain_id"])
    @append_query_filter(
        [
            "cost_id",
            "provider",
            "region_code",
            "region_key",
            "product",
            "usage_type",
            "resource",
            "service_account_id",
            "data_source_id",
            "project_id",
            "project_group_id",
            "user_projects",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["cost_id"])
    @set_query_page_limit(1000)
    @convert_model
    def list(self, params: dict) -> Union[CostsResponse, dict]:
        """List costs

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v2.Query)',
                'cost_id': 'str',
                'provider': 'str',
                'region_code': 'str',
                'region_key': 'str',
                'product': 'str',
                'usage_type': 'str',
                'resource': 'str',
                'service_account_id': 'str',
                'user_projects': 'list'                         # injected from auth(optional)
                'project_id': 'str',
                'data_source_id': 'str'                         # injected from auth
                'domain_id': 'str',
            }

        Returns:
            cost_vos (object)
            total_count
        """

        query = params.get("query", {})
        domain_id = params["domain_id"]
        data_source_id = params["data_source_id"]

        cost_vos, total_count = self.cost_mgr.list_costs(
            query, domain_id, data_source_id
        )

        # Check data fields permissions
        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            data_source_vo = self.data_source_mgr.get_data_source(
                data_source_id, domain_id
            )
            cost_reports_info = []
            for cost_vo in cost_vos:
                cost_info = cost_vo.to_dict()

                cost_info = self._remove_deny_fields_with_data_source_vo(
                    cost_info, data_source_vo
                )
                cost_reports_info.append(cost_info)
        else:
            cost_reports_info = [cost_vo.to_dict() for cost_vo in cost_vos]

        return CostsResponse(results=cost_reports_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:Cost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(
        [
            "query",
            "query.granularity",
            "query.start",
            "query.end",
            "query.fields",
            "data_source_id",
            "domain_id",
        ]
    )
    @append_query_filter(
        ["data_source_id", "user_projects", "workspace_id", "domain_id"]
    )
    @append_keyword_filter(["cost_id"])
    @set_query_page_limit(1000)
    def analyze(self, params: dict):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.TimeSeriesAnalyzeQuery)',
                'data_source_id': 'str',
                'domain_id': 'str',
                'user_projects': 'list' // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params["domain_id"]
        data_source_id = params["data_source_id"]
        query = params.get("query", {})
        workspace_id = query.get("workspace_id")

        if self.transaction.get_meta("authorization.role_type") != "DOMAIN_ADMIN":
            data_source_vo = self.data_source_mgr.get_data_source(
                data_source_id, domain_id, workspace_id
            )
            self._check_fields_with_data_source_permissions(query, data_source_vo)

        return self.cost_mgr.analyze_costs_by_granularity(
            query, domain_id, data_source_id
        )

    @transaction(
        permission="cost-analysis:Cost.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(
        ["data_source_id", "workspace_id", "domain_id", "user_projects"]
    )
    @append_keyword_filter(["cost_id"])
    @set_query_page_limit(1000)
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'data_source_id': 'str',
                'domain_id': 'str',
                'user_projects': 'list' // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params["domain_id"]
        query = params.get("query", {})
        data_source_id = self._get_data_source_id_from_query(params, query)

        if data_source_id and data_source_id != "global":
            query = self.cost_mgr.change_filter_v_workspace_id(
                query, domain_id, data_source_id
            )

        if self._is_distinct_query(query):
            page, query = self._get_page_from_query(query)
            search, query = self._get_search_value_from_query(query)
            query_hash = utils.dict_to_hash(query)

            self.cost_mgr.create_cost_query_history(
                query, query_hash, domain_id, data_source_id
            )

            response = self.cost_mgr.stat_monthly_costs_with_cache(
                query, query_hash, domain_id, data_source_id
            )

            if search:
                response = self._search_results(response, search)

            if page:
                response = self._page_results(response, page)

            return response
        else:
            raise ERROR_NOT_SUPPORT_QUERY_OPTION(query_option="aggregate")

    @staticmethod
    def _is_distinct_query(query):
        if "distinct" in query:
            return True
        else:
            return False

    @staticmethod
    def _get_page_from_query(query):
        if "page" in query:
            page = query["page"]
            del query["page"]
        else:
            page = None

        return page, query

    @staticmethod
    def _get_search_value_from_query(query):
        distinct = query["distinct"]

        search = None
        changed_filter = []
        for condition in query.get("filter", []):
            key = condition.get("key", condition.get("k"))
            value = condition.get("value", condition.get("v"))
            operator = condition.get("operator", condition.get("o"))

            if key == distinct and operator == "contain":
                search = value
            else:
                changed_filter.append(condition)

        query["filter"] = changed_filter

        return search, query

    @staticmethod
    def _search_results(response, search):
        search = search.lower()
        changed_results = []

        for result in response.get("results", []):
            if search in result.lower():
                changed_results.append(result)

        return {
            "results": changed_results,
        }

    @staticmethod
    def _page_results(response, page):
        results = response.get("results", [])
        response = {"total_count": len(results)}

        if "limit" in page and page["limit"] > 0:
            start = page.get("start", 1)
            if start < 1:
                start = 1

            response["results"] = results[start - 1 : start + page["limit"] - 1]
        else:
            response["results"] = results

        return response

    @staticmethod
    def _remove_deny_fields_with_data_source_vo(
        cost_info: dict, data_source_vo: DataSource
    ):
        permissions = data_source_vo.permissions or {}
        if permissions:
            deny = permissions.get("deny", [])
            for deny_field in deny:
                if utils.get_dict_value(cost_info, deny_field):
                    utils.change_dict_value(cost_info, deny_field, None)
        return cost_info

    @staticmethod
    def _check_fields_with_data_source_permissions(
        query: dict, data_source_vo: DataSource
    ):
        permissions = data_source_vo.permissions or {}
        deny = permissions.get("deny", [])

        fields = query.get("fields", {})

        for field_key, field_info in fields.items():
            if _field_info_key := field_info.get("key", field_info.get("k")):
                if _field_info_key in deny:
                    raise ERROR_PERMISSION_DENIED()

    @staticmethod
    def _check_workspace_id_with_cost_vo(
        cost_vo: Cost, domain_id: str, workspace_id: str
    ) -> None:
        if cost_vo.workspace_id.startswith("v-"):
            data_source_account_mgr = DataSourceAccountManager()
            data_source_account_vos = (
                data_source_account_mgr.filter_data_source_accounts(
                    domain_id=domain_id,
                    v_workspace_id=cost_vo.workspace_id,
                )
            )
            if not data_source_account_vos:
                raise ERROR_PERMISSION_DENIED()

            if workspace_id != data_source_account_vos[0].workspace_id:
                raise ERROR_PERMISSION_DENIED()

        elif cost_vo.workspace_id != workspace_id:
            raise ERROR_PERMISSION_DENIED()

    @staticmethod
    def _get_data_source_id_from_query(params: dict, query: dict):
        data_source_id = params.get("data_source_id", "global")
        if data_source_id == "global":
            for condition in query.get("filter", []):
                key = condition.get("k", condition.get("key"))
                value = condition.get("v", condition.get("value"))
                if key == "data_source_id":
                    data_source_id = value
        return data_source_id
