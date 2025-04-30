import logging
from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager import ReportAdjustmentPolicyManager
from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.report_adjustment_manager import (
    ReportAdjustmentManager,
)
from spaceone.cost_analysis.model.report_adjustment.request import *
from spaceone.cost_analysis.model.report_adjustment.response import *
from spaceone.cost_analysis.error import (
    ERROR_ADJUSTMENT_ORDER_BELOW_MINIMUM,
    ERROR_ADJUSTMENT_ORDER_EXCEEDS_MAXIMUM,
)

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ReportAdjustmentService(BaseService):
    resource = "ReportAdjustment"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adjustment_mgr = ReportAdjustmentManager()
        self.adjustment_policy_mgr = ReportAdjustmentPolicyManager()
        self.cost_report_config_mgr = CostReportConfigManager()

    @transaction(
        permission="cost-analysis:ReportAdjustment.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def create(self, params: CreateReportAdjustmentRequest) -> ReportAdjustmentResponse:
        """Create report adjustment

        Args:
            params (CreateReportAdjustmentRequest): {
                'name': 'str',                           # required
                'method': 'str',                         # required
                'value': 'float',                        # required
                'description': 'str',
                'provider': 'str',                       # required
                'currency': 'str',
                'order': 'int',
                'filter': 'dict',
                'report_adjustment_policy_id': 'str',    # required
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentResponse:
        """
        cost_report_config_id = self.get_cost_report_config_id_by_policy_id(
            params.report_adjustment_policy_id,
            params.domain_id,
        )

        existing_adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
            params.report_adjustment_policy_id, params.domain_id
        )

        if not params.order:
            if existing_adjustments:
                params.order = existing_adjustments[-1]["order"] + 1
            else:
                params.order = 1
        else:
            if not existing_adjustments:
                params.order = 1
            else:
                max_existing_order = existing_adjustments[-1]["order"]

                if params.order is None:
                    params.order = max_existing_order + 1

                if params.order > max_existing_order:
                    params.order = max_existing_order + 1
                else:
                    for adjustment in reversed(existing_adjustments):
                        if adjustment["order"] >= params.order:
                            adjustment["order"] += 1
                            self.adjustment_mgr.update_adjustment_order(
                                adjustment["report_adjustment_id"],
                                params.domain_id,
                                {"order": adjustment["order"]},
                            )

        params_dict = params.dict(exclude_unset=True)
        params_dict["cost_report_config_id"] = cost_report_config_id

        adjustment_vo = self.adjustment_mgr.create_adjustment(params_dict)
        return ReportAdjustmentResponse(**adjustment_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustment.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def update(self, params: UpdateReportAdjustmentRequest) -> ReportAdjustmentResponse:
        """Update report adjustment

        Args:
            params (UpdateReportAdjustmentRequest): {
                'report_adjustment_id': 'str',         # required
                'name': 'str',
                'method': 'str',
                'value': 'float',
                'description': 'str',
                'provider': 'str',
                'filter': 'dict',
                'domain_id': 'str'                     # injected from auth (required)
            }

        Returns:
            ReportAdjustmentResponse:
        """
        adjustment_vo = self.adjustment_mgr.get_adjustment(
            report_adjustment_id=params.report_adjustment_id, domain_id=params.domain_id
        )
        updated_adjustment_vo = self.adjustment_mgr.update_adjustment_by_vo(
            params.dict(exclude_unset=True), adjustment_vo
        )
        return ReportAdjustmentResponse(**updated_adjustment_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustment.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def change_order(
        self, params: ChangeOrderReportAdjustmentRequest
    ) -> ReportAdjustmentResponse:
        """Change order of report adjustment

        Args:
            params (ChangeOrderReportAdjustmentRequest): {
                'report_adjustment_id': 'str',              # required
                'order': 'int',                             # required
                'domain_id': 'str'                          # injected from auth (required)
            }

        Returns:
            ReportAdjustmentResponse:
        """
        if params.order < 1:
            raise ERROR_ADJUSTMENT_ORDER_BELOW_MINIMUM(order=params.order)

        adjustment_vo = self.adjustment_mgr.get_adjustment(
            report_adjustment_id=params.report_adjustment_id, domain_id=params.domain_id
        )

        if adjustment_vo.order == params.order:
            return ReportAdjustmentResponse(**adjustment_vo.to_dict())

        existing_adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
            adjustment_vo.report_adjustment_policy_id, params.domain_id
        )

        existing_adjustments = [
            adjustment
            for adjustment in existing_adjustments
            if adjustment["report_adjustment_id"] != params.report_adjustment_id
        ]

        max_order = existing_adjustments[-1]["order"] if existing_adjustments else 1

        if params.order > max_order:
            raise ERROR_ADJUSTMENT_ORDER_EXCEEDS_MAXIMUM(
                order=params.order, maximum_order=max_order
            )

        if adjustment_vo.order > params.order:
            for adjustment in existing_adjustments:
                if params.order <= adjustment["order"] < adjustment_vo.order:
                    adjustment["order"] += 1
                    self.adjustment_mgr.update_adjustment_order(
                        adjustment["report_adjustment_id"],
                        params.domain_id,
                        {"order": adjustment["order"]},
                    )
        elif adjustment_vo.order < params.order:
            for adjustment in existing_adjustments:
                if adjustment_vo.order < adjustment["order"] <= params.order:
                    adjustment["order"] -= 1
                    self.adjustment_mgr.update_adjustment_order(
                        adjustment["report_adjustment_id"],
                        params.domain_id,
                        {"order": adjustment["order"]},
                    )

        updated_adjustment_vo = self.adjustment_mgr.update_adjustment_order(
            adjustment_vo.report_adjustment_id,
            params.domain_id,
            {"order": params.order},
        )

        return ReportAdjustmentResponse(**updated_adjustment_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustment.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def delete(self, params: ReportAdjustmentDeleteRequest) -> None:
        """Delete report adjustment

        Args:
            params (ReportAdjustmentDeleteRequest): {
                'report_adjustment_id': 'str',         # required
                'domain_id': 'str'                     # injected from auth (required)
            }

        Returns:
            None:
        """
        adjustment_vo = self.adjustment_mgr.get_adjustment(
            report_adjustment_id=params.report_adjustment_id, domain_id=params.domain_id
        )
        self.adjustment_mgr.delete_adjustment_by_vo(adjustment_vo)

    @transaction(
        permission="cost-analysis:ReportAdjustment.read", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def get(self, params: ReportAdjustmentGetRequest) -> ReportAdjustmentResponse:
        """Get report adjustment

        Args:
            params (ReportAdjustmentGetRequest): {
                'report_adjustment_id': 'str',       # required
                'domain_id': 'str'                   # injected from auth (required)
            }

        Returns:
            ReportAdjustmentResponse:
        """
        adjustment_vo = self.adjustment_mgr.get_adjustment(
            report_adjustment_id=params.report_adjustment_id, domain_id=params.domain_id
        )
        return ReportAdjustmentResponse(**adjustment_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustment.read", role_types=["DOMAIN_ADMIN"]
    )
    @append_query_filter(
        [
            "provider",
            "report_adjustment_id",
            "report_adjustment_policy_id",
            "cost_report_config_id",
            "domain_id",
        ]
    )
    @convert_model
    def list(
        self, params: ReportAdjustmentSearchQueryRequest
    ) -> ReportAdjustmentsResponse:
        """List report adjustments

        Args:
            params (ReportAdjustmentSearchQueryRequest): {
                'query': 'dict',
                'provider': 'str',
                'report_adjustment_id': 'str',
                'report_adjustment_policy_id': 'str',
                'cost_report_config_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            ReportAdjustmentsResponse:
        """
        query = params.dict() or {}
        adjustment_vos, total_count = self.adjustment_mgr.list_adjustments(query)
        results = [adj.to_dict() for adj in adjustment_vos]
        return ReportAdjustmentsResponse(results=results, total_count=total_count)

    def get_cost_report_config_id_by_policy_id(
        self,
        report_adjustment_policy_id: str,
        domain_id: str,
    ):
        adjustment_policy_vo = self.adjustment_policy_mgr.get_policy(
            policy_id=report_adjustment_policy_id,
            domain_id=domain_id,
        )
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id=domain_id,
            cost_report_config_id=adjustment_policy_vo.cost_report_config_id,
        )
        return cost_report_config_vo.cost_report_config_id
