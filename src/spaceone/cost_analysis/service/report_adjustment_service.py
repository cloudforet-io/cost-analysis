import logging
from typing import Union

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
    def create(
        self, params: CreateReportAdjustmentRequest
    ) -> Union[ReportAdjustmentResponse, dict]:
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
                'adjustment_filter': 'dict',
                'report_adjustment_policy_id': 'str',    # required
                'domain_id': 'str'                       # injected from auth (required)
            }

        Returns:
            ReportAdjustmentResponse:
        """

        adjustment_policy_vo = self.adjustment_policy_mgr.get_policy(
            policy_id=params.report_adjustment_policy_id,
            domain_id=params.domain_id,
        )
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            domain_id=params.domain_id,
            cost_report_config_id=adjustment_policy_vo.cost_report_config_id,
        )

        existing_adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
            params.report_adjustment_policy_id, params.domain_id
        )

        params.order = self._assign_order_to_new_adjustment(
            params, existing_adjustments
        )

        params_dict = params.dict(exclude_unset=True)
        params_dict["cost_report_config_id"] = (
            cost_report_config_vo.cost_report_config_id
        )

        adjustment_vo = self.adjustment_mgr.create_adjustment(params_dict)

        self._update_policy_adjustment_order_list(
            adjustment_policy_vo, params.domain_id
        )

        return ReportAdjustmentResponse(**adjustment_vo.to_dict())

    @transaction(
        permission="cost-analysis:ReportAdjustment.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def update(
        self, params: UpdateReportAdjustmentRequest
    ) -> Union[ReportAdjustmentResponse, dict]:
        """Update report adjustment

        Args:
            params (UpdateReportAdjustmentRequest): {
                'report_adjustment_id': 'str',         # required
                'name': 'str',
                'method': 'str',
                'value': 'float',
                'description': 'str',
                'provider': 'str',
                'adjustment_filter': 'dict',
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
    ) -> Union[ReportAdjustmentResponse, dict]:
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

        max_order = existing_adjustments[-1]["order"] if existing_adjustments else 1

        if params.order > max_order:
            raise ERROR_ADJUSTMENT_ORDER_EXCEEDS_MAXIMUM(
                order=params.order, maximum_order=max_order
            )

        existing_adjustments = [
            adjustment
            for adjustment in existing_adjustments
            if adjustment["report_adjustment_id"] != params.report_adjustment_id
        ]

        self._reorder_adjustments(
            adjustments=existing_adjustments,
            adjustment_vo=adjustment_vo,
            new_order=params.order,
            domain_id=params.domain_id,
        )

        updated_adjustment_vo = self.adjustment_mgr.update_adjustment_order(
            adjustment_vo.report_adjustment_id,
            params.domain_id,
            {"order": params.order},
        )

        adjustment_policy_vo = self.adjustment_policy_mgr.get_policy(
            policy_id=adjustment_vo.report_adjustment_policy_id,
            domain_id=params.domain_id,
        )

        self._update_policy_adjustment_order_list(
            adjustment_policy_vo, params.domain_id
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

        adjustment_policy_vo = self.adjustment_policy_mgr.get_policy(
            policy_id=adjustment_vo.report_adjustment_policy_id,
            domain_id=params.domain_id,
        )

        self._reindex_orders(
            adjustment_policy_vo,
            params.domain_id,
            exclude_adjustment_id=adjustment_vo.report_adjustment_id,
        )

        self._update_policy_adjustment_order_list(
            adjustment_policy_vo,
            params.domain_id,
            exclude_adjustment_id=adjustment_vo.report_adjustment_id,
        )

        self.adjustment_mgr.delete_adjustment_by_vo(adjustment_vo)

    @transaction(
        permission="cost-analysis:ReportAdjustment.read", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def get(
        self, params: ReportAdjustmentGetRequest
    ) -> Union[ReportAdjustmentResponse, dict]:
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
    ) -> Union[ReportAdjustmentsResponse, dict]:
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
        query = params.query or {}
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

    def _assign_order_to_new_adjustment(self, params, existing_adjustments):
        if not params.order:
            return existing_adjustments[-1]["order"] + 1 if existing_adjustments else 1
        else:
            if not existing_adjustments:
                return 1
            max_existing_order = existing_adjustments[-1]["order"]

            if params.order > max_existing_order:
                return max_existing_order + 1

            for adjustment in reversed(existing_adjustments):
                if adjustment["order"] >= params.order:
                    adjustment["order"] += 1
                    self.adjustment_mgr.update_adjustment_order(
                        adjustment["report_adjustment_id"],
                        params.domain_id,
                        {"order": adjustment["order"]},
                    )
            return params.order

    def _reorder_adjustments(self, adjustments, adjustment_vo, new_order, domain_id):
        if adjustment_vo.order > new_order:
            for adjustment in adjustments:
                if new_order <= adjustment["order"] < adjustment_vo.order:
                    adjustment["order"] += 1
                    self.adjustment_mgr.update_adjustment_order(
                        adjustment["report_adjustment_id"],
                        domain_id,
                        {"order": adjustment["order"]},
                    )
        else:
            for adjustment in adjustments:
                if adjustment_vo.order < adjustment["order"] <= new_order:
                    adjustment["order"] -= 1
                    self.adjustment_mgr.update_adjustment_order(
                        adjustment["report_adjustment_id"],
                        domain_id,
                        {"order": adjustment["order"]},
                    )

    def _reindex_orders(self, policy_vo, domain_id, exclude_adjustment_id=None):
        adjustments = self.adjustment_mgr.filter_adjustments(
            report_adjustment_policy_id=policy_vo.report_adjustment_policy_id,
            domain_id=domain_id,
        )

        sorted_adjustments = sorted(adjustments, key=lambda x: x.order)
        sorted_adjustments = [
            adjustment
            for adjustment in sorted_adjustments
            if adjustment
            if adjustment.report_adjustment_id != exclude_adjustment_id
        ]

        for idx, adj in enumerate(sorted_adjustments, start=1):
            self.adjustment_mgr.update_adjustment_order(
                adj.report_adjustment_id, domain_id, {"order": idx}
            )

    def _update_policy_adjustment_order_list(
        self, policy_vo, domain_id, exclude_adjustment_id=None
    ):
        sorted_adjustments = [
            adj
            for adj in self.adjustment_mgr.filter_adjustments(
                report_adjustment_policy_id=policy_vo.report_adjustment_policy_id,
                domain_id=domain_id,
            )
            if adj.report_adjustment_id != exclude_adjustment_id
        ]

        sorted_adjustments.sort(key=lambda x: x.order)

        adjustment_ids_by_order = [
            adj.report_adjustment_id for adj in sorted_adjustments
        ]

        self.adjustment_policy_mgr.update_policy_by_vo(
            {"adjustments": adjustment_ids_by_order}, policy_vo
        )
