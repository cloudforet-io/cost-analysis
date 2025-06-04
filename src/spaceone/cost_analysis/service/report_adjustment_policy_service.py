import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.error.report_adjustment_policy import *
from spaceone.cost_analysis.manager import (
    CostReportConfigManager,
    ReportAdjustmentPolicyManager,
    ReportAdjustmentManager,
)
from spaceone.cost_analysis.model.report_adjustment_policy.request import *
from spaceone.cost_analysis.model.report_adjustment_policy.response import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ReportAdjustmentPolicyService(BaseService):
    resource = "ReportAdjustmentPolicy"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adjustment_mgr = ReportAdjustmentManager()
        self.policy_mgr = ReportAdjustmentPolicyManager()
        self.cost_report_config_mgr = CostReportConfigManager()

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @convert_model
    def create(
        self, params: CreateReportAdjustmentPolicyRequest
    ) -> Union[ReportAdjustmentPolicyResponse, dict]:
        """Create report adjustment policy

        Args:
            params (CreateReportAdjustmentPolicyRequest): {
                'cost_report_config_id': 'str',   # required
                'order': 'int',
                'description': 'str',
                'tags': 'dict',
                'policy_filter': 'dict',
                'domain_id': 'str'                 # injected from auth (required)
            }

        Returns:
            ReportAdjustmentPolicyResponse:
        """

        params_dict = params.dict()

        domain_id = params_dict["domain_id"]
        cost_report_config_id = params_dict["cost_report_config_id"]

        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            cost_report_config_id=cost_report_config_id, domain_id=domain_id
        )

        scope = cost_report_config_vo.scope

        if scope != "WORKSPACE":
            policy_filter = params.policy_filter or {}
            workspace_ids = policy_filter.get("workspace_ids", [])

            if scope == "PROJECT":
                project_ids = policy_filter.get("project_ids", [])

                if not workspace_ids or not project_ids:
                    raise ERROR_PROJECT_OR_WORKSPACE_REQUIRED(
                        scope=scope,
                        workspace_ids=workspace_ids,
                        project_ids=project_ids,
                    )
            elif scope == "SERVICE_ACCOUNT":
                service_account_ids = policy_filter.get("service_account_ids", [])

                if not workspace_ids or not service_account_ids:
                    raise ERROR_SERVICE_ACCOUNT_OR_WORKSPACE_REQUIRED(
                        scope=scope,
                        workspace_ids=workspace_ids,
                        service_account_ids=service_account_ids,
                    )

        self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id, params.cost_report_config_id
        )

        existing_policies = self.policy_mgr.list_sorted_policies_by_order(
            params.cost_report_config_id, params.domain_id
        )

        if not params_dict.get("order"):
            if existing_policies:
                params_dict["order"] = existing_policies[-1]["order"] + 1
            else:
                params_dict["order"] = 1

        else:
            if not existing_policies:
                params_dict["order"] = 1
            else:
                max_existing_order = existing_policies[-1]["order"]

                if params_dict.get("order") is None:
                    params_dict["order"] = max_existing_order + 1

                if params_dict["order"] > max_existing_order:
                    params_dict["order"] = max_existing_order + 1
                else:
                    for policy in reversed(existing_policies):
                        if policy["order"] >= params.order:
                            policy["order"] += 1
                            self.policy_mgr.update_policy_order(
                                policy["report_adjustment_policy_id"],
                                params.domain_id,
                                {"order": policy["order"]},
                            )

        params_dict["scope"] = scope
        policy_vo = self.policy_mgr.create_policy(params_dict)
        return ReportAdjustmentPolicyResponse(**policy_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @convert_model
    def update(
        self, params: UpdateReportAdjustmentPolicyRequest
    ) -> Union[ReportAdjustmentPolicyResponse, dict]:
        """Update report adjustment policy

        Args:
            params (UpdateReportAdjustmentPolicyRequest): {
                'report_adjustment_policy_id': 'str',    # required
                'description': 'str',
                'tags': 'dict',
                'policy_filter': 'dict',
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentPolicyResponse:
        """
        policy_vo = self.policy_mgr.get_policy(
            policy_id=params.report_adjustment_policy_id, domain_id=params.domain_id
        )
        updated_vo = self.policy_mgr.update_policy_by_vo(params.dict(), policy_vo)
        return ReportAdjustmentPolicyResponse(**updated_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @convert_model
    def change_order(
        self, params: ChangeOrderReportAdjustmentPolicyRequest
    ) -> Union[ReportAdjustmentPolicyResponse, dict]:
        """Change order of report adjustment policy

        Args:
            params (ChangeOrderReportAdjustmentPolicyRequest): {
                'report_adjustment_policy_id': 'str',    # required
                'order': 'int',                          # required
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentPolicyResponse:
        """
        if params.order < 1:
            raise ERROR_POLICY_ORDER_BELOW_MINIMUM(order=params.order)

        policy_vo = self.policy_mgr.get_policy(
            params.report_adjustment_policy_id, params.domain_id
        )

        if policy_vo.order == params.order:
            return ReportAdjustmentPolicyResponse(**policy_vo.to_dict())

        existing_policies = self.policy_mgr.list_sorted_policies_by_order(
            policy_vo.cost_report_config_id, params.domain_id
        )

        max_existing_order = existing_policies[-1]["order"] if existing_policies else 1

        if params.order > max_existing_order:
            raise ERROR_POLICY_ORDER_EXCEEDS_MAXIMUM(
                order=params.order, maximum_order=max_existing_order
            )

        existing_policies = [
            policy
            for policy in existing_policies
            if policy["report_adjustment_policy_id"]
               != params.report_adjustment_policy_id
        ]

        if policy_vo.order > params.order:
            for policy in existing_policies:
                if params.order <= policy["order"] < policy_vo.order:
                    policy["order"] += 1
                    self.policy_mgr.update_policy_order(
                        policy["report_adjustment_policy_id"],
                        params.domain_id,
                        {"order": policy["order"]},
                    )

        elif policy_vo.order < params.order:
            for policy in existing_policies:
                if policy_vo.order < policy["order"] <= params.order:
                    policy["order"] -= 1
                    self.policy_mgr.update_policy_order(
                        policy["report_adjustment_policy_id"],
                        params.domain_id,
                        {"order": policy["order"]},
                    )

        updated_vo = self.policy_mgr.update_policy_order(
            policy_vo.report_adjustment_policy_id,
            params.domain_id,
            {"order": params.order},
        )
        return ReportAdjustmentPolicyResponse(**updated_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @convert_model
    def delete(self, params: ReportAdjustmentPolicyDeleteRequest) -> None:
        """Delete report adjustment policy

        Args:
            params (ReportAdjustmentPolicyDeleteRequest): {
                'report_adjustment_policy_id': 'str',         # required
                'domain_id': 'str'                            # injected from auth (required)
            }

        Returns:
            None:
        """
        policy_vo = self.policy_mgr.get_policy(
            policy_id=params.report_adjustment_policy_id, domain_id=params.domain_id
        )

        if policy_vo.adjustments:
            for adjustment_id in policy_vo.adjustments:
                self.adjustment_mgr.delete_adjustment_by_id(
                    adjustment_id, params.domain_id
                )

        self.policy_mgr.delete_policy_by_vo(policy_vo)

        existing_policies = self.policy_mgr.list_sorted_policies_by_order(
            policy_vo.cost_report_config_id, params.domain_id
        )

        for idx, policy in enumerate(existing_policies, start=1):
            self.policy_mgr.update_policy_order(
                policy["report_adjustment_policy_id"],
                params.domain_id,
                {"order": idx},
            )

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @convert_model
    def sync_currency(
        self, params: ReportAdjustmentPolicySyncCurrencyRequest
    ) -> Union[ReportAdjustmentPolicyResponse, dict]:
        """
        Args:
            params (ReportAdjustmentPolicySyncCurrencyRequest): {
                'report_adjustment_policy_id': 'str',    # required
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentPolicyResponse:

        """

        domain_id = params.domain_id
        report_adj_policy_id = params.report_adjustment_policy_id

        policy_vo = self.policy_mgr.get_policy(
            policy_id=report_adj_policy_id, domain_id=domain_id
        )

        cost_report_config_id = policy_vo.cost_report_config_id
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id, cost_report_config_id
        )

        adjustment_vos = self.adjustment_mgr.filter_adjustments(
            report_adjustment_policy_id=report_adj_policy_id,
            domain_id=domain_id,
            cost_report_config_id=cost_report_config_id,
        )

        for adjustment_vo in adjustment_vos:
            if cost_report_config_vo.currency != adjustment_vo.currency:
                self.policy_mgr.update_policy_by_vo(
                    {"currency": cost_report_config_vo.currency}, adjustment_vo
                )

        return ReportAdjustmentPolicyResponse(**policy_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get(
        self, params: ReportAdjustmentPolicyGetRequest
    ) -> Union[ReportAdjustmentPolicyResponse, dict]:
        """Get report adjustment policy

        Args:
            params (ReportAdjustmentPolicyGetRequest): {
                'report_adjustment_policy_id': 'str',    # required
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentPolicyResponse:
        """
        policy_vo = self.policy_mgr.get_policy(
            policy_id=params.report_adjustment_policy_id, domain_id=params.domain_id
        )
        return ReportAdjustmentPolicyResponse(**policy_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustmentPolicy.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "domain_id",
            "report_adjustment_policy_id",
        ]
    )
    @convert_model
    def list(
        self, params: ReportAdjustmentPolicySearchQueryRequest
    ) -> Union[ReportAdjustmentPoliciesResponse, dict]:
        """List report adjustment policies

        Args:
            params (ReportAdjustmentPolicySearchQueryRequest): {
                'query': 'dict',
                'report_adjustment_policy_id': 'str',
                'domain_id': 'str',
            }

        Returns:
            ReportAdjustmentPoliciesResponse:
        """

        query = params.query or {}
        policy_vos, total_count = self.policy_mgr.list_policies(query)

        results = [policy_vo.to_dict() for policy_vo in policy_vos]
        return ReportAdjustmentPoliciesResponse(
            results=results, total_count=total_count
        )
