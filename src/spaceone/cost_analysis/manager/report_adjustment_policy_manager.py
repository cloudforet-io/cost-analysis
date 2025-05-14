import logging
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model import ReportAdjustmentPolicy

_LOGGER = logging.getLogger(__name__)


class ReportAdjustmentPolicyManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy_model = ReportAdjustmentPolicy

    def create_policy(self, params: dict) -> ReportAdjustmentPolicy:
        def _rollback(vo: ReportAdjustmentPolicy):
            _LOGGER.info(
                f"[create_policy._rollback] Delete policy: {vo.report_adjustment_policy_id}"
            )
            vo.delete()

        policy_vo = self.policy_model.create(params)
        self.transaction.add_rollback(_rollback, policy_vo)
        return policy_vo

    def update_policy_by_vo(
        self, params: dict, policy_vo: ReportAdjustmentPolicy
    ) -> ReportAdjustmentPolicy:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_policy_by_vo._rollback] Revert Data: {old_data['report_adjustment_policy_id']}"
            )
            policy_vo.update(old_data)

        self.transaction.add_rollback(_rollback, policy_vo.to_dict())
        return policy_vo.update(params)

    def update_policy_order(
        self, policy_id: str, domain_id: str, oder_dict: dict
    ) -> ReportAdjustmentPolicy:
        policy_vo = self.get_policy(policy_id=policy_id, domain_id=domain_id)

        def _rollback(old_data):
            _LOGGER.info(
                f"[update_policy_by_id._rollback] Revert Data: {old_data['report_adjustment_policy_id']}"
            )
            policy_vo.update(old_data)

        self.transaction.add_rollback(_rollback, policy_vo.to_dict())
        policy_vo.update(oder_dict)
        policy_vo.save()
        return policy_vo

    @staticmethod
    def delete_policy_by_vo(policy_vo: ReportAdjustmentPolicy) -> None:
        policy_vo.delete()

    def get_policy(self, policy_id: str, domain_id: str) -> ReportAdjustmentPolicy:
        return self.policy_model.get(
            report_adjustment_policy_id=policy_id, domain_id=domain_id
        )

    def filter_policies(self, **conditions):
        return self.policy_model.filter(**conditions)

    def list_policies(self, query: dict):
        return self.policy_model.query(**query)

    def list_sorted_policies_by_order(
        self, cost_report_config_id: str, domain_id: str
    ) -> list:
        query = {
            "filter": [
                {"k": "cost_report_config_id", "v": cost_report_config_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ],
            "order_by": ["order"],
        }
        policy_vos, _ = self.list_policies(query)

        policies = [policy_vo.to_dict() for policy_vo in policy_vos]

        sorted_policies = sorted(policies, key=lambda x: x.get("order", 9999))

        return sorted_policies

    def stat_policies(self, query: dict):
        return self.policy_model.stat(**query)
