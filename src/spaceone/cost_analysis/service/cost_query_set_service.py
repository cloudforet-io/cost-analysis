import logging
from typing import Union

from ecdsa.test_pyecdsa import params
from spaceone.core.service import *

from spaceone.cost_analysis.manager import DataSourceManager
from spaceone.cost_analysis.manager.cost_query_set_manager import CostQuerySetManager
from spaceone.cost_analysis.model.cost_query_set.database import CostQuerySet
from spaceone.cost_analysis.model.cost_query_set.request import CostQuerySetCreateRequest, CostQuerySetUpdateRequest, \
    CostQuerySetDeleteRequest, CostQuerySetGetRequest, CostQuerySetSearchQueryRequest, CostQuerySetStatQueryRequest
from spaceone.cost_analysis.model.cost_query_set.response import CostQuerySetResponse, CostQuerySetsResponse

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostQuerySetService(BaseService):
    resource = "CostQuerySet"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr = DataSourceManager()
        self.cost_query_set_mgr = CostQuerySetManager()

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["data_source_id", "name", "options", "user_id", "domain_id"])
    @change_date_value(["start", "end"])
    def create(self, params: CostQuerySetCreateRequest) -> Union[CostQuerySetResponse, dict]:
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

        domain_id = params.domain_id
        data_source_id = params.data_source_id

        if data_source_id != "unified-cost-data-source":
            self.data_source_mgr.get_data_source(
                domain_id=domain_id, data_source_id=data_source_id
            )

        cost_query_set_vo = self.cost_query_set_mgr.create_cost_query_set(params.dict(exclude_unset=True))
        return CostQuerySetResponse(**cost_query_set_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    @change_date_value(["end"])
    def update(self, params: CostQuerySetUpdateRequest) -> Union[CostQuerySetResponse, dict]:
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

        cost_query_set_id = params.cost_query_set_id
        user_id = params.user_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        cost_query_set_vo = self.cost_query_set_mgr.get_cost_query_set(
            cost_query_set_id, user_id, domain_id, workspace_id
        )

        updated_cost_query_set_vo = self.cost_query_set_mgr.update_cost_query_set_by_vo(
            params, cost_query_set_vo
        )

        return CostQuerySetResponse(**updated_cost_query_set_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostQuerySet.write",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    def delete(self, params: CostQuerySetDeleteRequest) -> None:
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
            params.cost_query_set_id,
            params.user_id,
            params.domain_id,
            params.workspace_id,
        )

        self.cost_query_set_mgr.delete_cost_query_set_by_vo(cost_query_set_vo)

    @transaction(
        permission="cost-analysis:CostQuerySet.read",
        role_types=["USER"],
    )
    @check_required(["cost_query_set_id", "user_id", "domain_id"])
    def get(self, params: CostQuerySetGetRequest) -> Union[CostQuerySetResponse, dict]:
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

        cost_query_set_id = params.cost_query_set_id
        user_id = params.user_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        cost_query_set_vo = self.cost_query_set_mgr.get_cost_query_set(
            cost_query_set_id, user_id, domain_id, workspace_id
        )

        return CostQuerySetResponse(**cost_query_set_vo.to_dict())

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
    def list(self, params: CostQuerySetSearchQueryRequest) -> Union[CostQuerySetsResponse, dict]:
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
        query = params.query or {}

        cost_query_set_data_vos, total_count= self.cost_query_set_mgr.list_cost_query_sets(query)
        cost_query_sets_data_info = [cost_query_set_data_vo.to_dict() for cost_query_set_data_vo in cost_query_set_data_vos]
        return CostQuerySetsResponse(results=cost_query_sets_data_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:CostQuerySet.read",
        role_types=["USER"],
    )
    @check_required(["query", "data_source_id", "domain_id"])
    @append_query_filter(["data_source_id", "domain_id"])
    @append_keyword_filter(["cost_query_set_id", "name"])
    def stat(self, params: CostQuerySetStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.query or {}
        return self.cost_query_set_mgr.stat_cost_query_sets(query)
