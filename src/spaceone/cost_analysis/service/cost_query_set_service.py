import logging

from spaceone.core.service import *
from spaceone.cost_analysis.manager.cost_query_set_manager import CostQuerySetManager
from spaceone.cost_analysis.model.cost_query_set_model import CostQuerySet

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostQuerySetService(BaseService):
    resource = "CostQuerySet"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_query_set_mgr: CostQuerySetManager = self.locator.get_manager(
            "CostQuerySetManager"
        )

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["data_source_id", "name", "options", "user_id", "domain_id"])
    @change_date_value(["start", "end"])
    def create(self, params):
        """Register cost_query_set

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'options': 'str',
                'tags': 'dict',
                'user_id': 'str',           # injected from auth
                'workspace_id': 'str',      # injected from auth (optional)
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            cost_query_set_vo (object)
        """

        return self.cost_query_set_mgr.create_cost_query_set(params)

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    @change_date_value(["end"])
    def update(self, params: dict):
        """Update cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'name': 'str',
                'options': 'dict',
                'tags': 'dict'
                'user_id': 'str',               # injected from auth
                'workspace_id': 'str',          # injected from auth (optional)
                'domain_id': 'str'              # injected from auth
            }

        Returns:
            cost_query_set_vo (object)
        """
        cost_query_set_id = params["cost_query_set_id"]
        user_id = params["user_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        cost_query_set_vo: CostQuerySet = self.cost_query_set_mgr.get_cost_query_set(
            cost_query_set_id, user_id, domain_id, workspace_id
        )

        return self.cost_query_set_mgr.update_cost_query_set_by_vo(
            params, cost_query_set_vo
        )

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    def delete(self, params: dict):
        """Deregister cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            None
        """

        cost_query_set_vo = self.cost_query_set_mgr.get_cost_query_set(
            params["cost_query_set_id"],
            params["user_id"],
            params["domain_id"],
            params.get("workspace_id"),
        )

        self.cost_query_set_mgr.delete_cost_query_set_by_vo(cost_query_set_vo)

    @transaction(
        permission="cost-analysis:CostQuerySet.read",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    def get(self, params: dict):
        """Get cost_query_set

        Args:
            params (dict): {
                'cost_query_set_id': 'str',
                'user_id': 'str',               # injected from auth
                'domain_id': 'str'             # injected from auth
            }

        Returns:
            cost_query_set_vo (object)
        """

        cost_query_set_id = params["cost_query_set_id"]
        user_id = params["user_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        return self.cost_query_set_mgr.get_cost_query_set(
            cost_query_set_id, user_id, domain_id, workspace_id
        )

    @transaction(
        permission="cost-analysis:CostQuerySet.read",
        role_types=["USER"],
    )
    @check_required(["data_source_id", "user_id", "domain_id"])
    @append_query_filter(
        [
            "data_source_id",
            "cost_query_set_id",
            "name",
            "user_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["cost_query_set_id", "name"])
    def list(self, params: dict):
        """List cost_query_sets

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v2.Query)'
                'data_source_id': 'str',
                'cost_query_set_id': 'str',
                'name': 'str',
                'user_id': 'str',                               # injected from auth
                'workspace_id': 'str',                          # injected from auth (optional)
                'domain_id': 'str',                             # injected from auth

            }

        Returns:
            cost_query_set_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.cost_query_set_mgr.list_cost_query_sets(query)

    @transaction(
        permission="cost-analysis:CostQuerySet.read",
        role_types=["USER"],
    )
    @check_required(["query", "data_source_id", "domain_id"])
    @append_query_filter(["data_source_id", "domain_id"])
    @append_keyword_filter(["cost_query_set_id", "name"])
    def stat(self, params: dict):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.cost_query_set_mgr.stat_cost_query_sets(query)
