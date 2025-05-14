import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model import ReportAdjustment

_LOGGER = logging.getLogger(__name__)


class ReportAdjustmentManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adjustment_model = ReportAdjustment

    def create_adjustment(self, params: dict) -> ReportAdjustment:
        def _rollback(vo: ReportAdjustment):
            _LOGGER.info(f"Rollback: Delete adjustment ({vo.report_adjustment_id})")
            vo.delete()

        adjustment_vo = self.adjustment_model.create(params)
        self.transaction.add_rollback(_rollback, adjustment_vo)
        return adjustment_vo

    def update_adjustment_by_vo(
        self, params: dict, adjustment_vo: ReportAdjustment
    ) -> ReportAdjustment:
        def _rollback(old_data):
            _LOGGER.info(
                f"Rollback: Revert adjustment ({adjustment_vo.report_adjustment_id})"
            )
            adjustment_vo.update(old_data)

        self.transaction.add_rollback(_rollback, adjustment_vo.to_dict())
        adjustment_vo.update(params)
        adjustment_vo.save()
        return adjustment_vo

    def update_adjustment_order(
        self, adjustment_id: str, domain_id: str, order_dict: dict
    ) -> ReportAdjustment:
        adjustment_vo = self.get_adjustment(
            report_adjustment_id=adjustment_id, domain_id=domain_id
        )

        def _rollback(old_data):
            _LOGGER.info(
                f"Rollback: Revert adjustment ({old_data['report_adjustment_id']})"
            )
            adjustment_vo.update(old_data)

        self.transaction.add_rollback(_rollback, adjustment_vo.to_dict())
        adjustment_vo.update(order_dict)
        adjustment_vo.save()
        return adjustment_vo

    def delete_adjustment_by_vo(self, adjustment_vo: ReportAdjustment) -> None:
        def _rollback():
            _LOGGER.info(
                f"Rollback: Revive adjustment ({adjustment_vo.report_adjustment_id})"
            )

        self.transaction.add_rollback(_rollback)
        adjustment_vo.delete()

    def delete_adjustment_by_id(self, adjustment_id: str, domain_id: str) -> None:
        adjustment_vo = self.get_adjustment(
            report_adjustment_id=adjustment_id, domain_id=domain_id
        )

        def _rollback():
            _LOGGER.info(
                f"Rollback: Revive adjustment ({adjustment_vo.report_adjustment_id})"
            )

        self.transaction.add_rollback(_rollback)
        adjustment_vo.delete()

    def get_adjustment(
        self, report_adjustment_id: str, domain_id: str
    ) -> ReportAdjustment:
        return self.adjustment_model.get(
            report_adjustment_id=report_adjustment_id, domain_id=domain_id
        )

    def list_adjustments(self, query: dict) -> Tuple[QuerySet, int]:
        _LOGGER.debug(f"[list_adjustments] query: {query}")
        return self.adjustment_model.query(**query)

    def list_sorted_adjustments_by_order(
        self, report_adjustment_policy_id: str, domain_id: str
    ) -> list:
        query = {
            "filter": [
                {
                    "k": "report_adjustment_policy_id",
                    "v": report_adjustment_policy_id,
                    "o": "eq",
                },
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ],
            "order_by": ["order"],
        }

        adjustment_vos, _ = self.list_adjustments(query)
        adjustments = [adjustment_vo.to_dict() for adjustment_vo in adjustment_vos]
        sorted_adjustments = sorted(adjustments, key=lambda x: x.get("order", 9999))
        return sorted_adjustments

    def filter_adjustments(self, **conditions) -> QuerySet:
        return self.adjustment_model.objects.filter(**conditions)

    def reindex_adjustments(self, policy_id: str, domain_id: str) -> None:
        adjustments = self.filter_adjustments(
            report_adjustment_policy_id=policy_id, domain_id=domain_id
        ).order_by("order")

        for idx, adjustment in enumerate(adjustments, start=1):
            adjustment.update({"order": idx})
