import logging

from datetime import datetime
from decimal import Decimal
from spaceone.cost_analysis.manager import (
    ReportAdjustmentManager,
    ReportAdjustmentPolicyManager,
    CostReportDataManager,
    UnifiedCostManager,
)
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager

_LOGGER = logging.getLogger(__name__)

CURRENCIES = [
    "USD",
    "KRW",
    "JPY",
]


class AdjustmentPolicyApplier:
    def __init__(self, cost_report_vo, data_source_ids):
        self.policy_mgr = ReportAdjustmentPolicyManager()
        self.adjustment_mgr = ReportAdjustmentManager()
        self.cost_report_data_mgr = CostReportDataManager()
        self.unified_cost_mgr = UnifiedCostManager()
        self.currency_mgr = CurrencyManager()

        self.cost_report_vo = cost_report_vo

        self.config_id = cost_report_vo.cost_report_config_id
        self.report_month = cost_report_vo.report_month
        self.currency_date = cost_report_vo.currency_date
        self.issue_date = cost_report_vo.issue_date
        self.domain_id = cost_report_vo.domain_id
        self.workspace_name = cost_report_vo.workspace_name
        self.workspace_id = cost_report_vo.workspace_id
        self.project_id = cost_report_vo.project_id

        self.data_source_ids = data_source_ids

        self.policies = []
        self.is_applied = self._check_applicable_policies()

    def apply_policies(self) -> None:
        policies = self.policy_mgr.list_sorted_policies_by_order(
            cost_report_config_id=self.config_id,
            domain_id=self.domain_id,
        )

        for policy in policies:
            if not self._is_policy_applicable(policy):
                continue

            _LOGGER.debug(f"Applying policy: {policy['name']}")
            report_adjustment_policy_id = policy["report_adjustment_policy_id"]
            adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
                report_adjustment_policy_id, self.domain_id
            )

            if not adjustments:
                continue

            if all(adj["method"] == "FIXED" for adj in adjustments):
                self._create_cost_report_data_for_fixed_only(
                    adjustments, report_adjustment_policy_id
                )
            else:
                self._create_cost_report_data_for_all_methods(
                    adjustments, report_adjustment_policy_id
                )

            self.is_applied = True

    def _create_cost_report_data_for_fixed_only(
        self, adjustments: list, report_adjustment_policy_id: str
    ) -> None:
        currency_map, _ = self.currency_mgr.get_currency_map_date(
            currency_end_date=self.currency_date,
        )

        for adjustment in adjustments:
            adjusted_cost = {}
            value = adjustment.get("value")
            provider = adjustment.get("provider")
            product = adjustment.get("name")
            currency = adjustment.get("currency")
            fixed_value = self.unified_cost_mgr.get_exchange_currency(
                value, currency, currency_map
            )
            for cur, val in fixed_value.items():
                adjusted_cost[cur] = float(adjusted_cost.get(cur, 0)) + float(val)

            cost_report_data = self._build_cost_report_data_dict(
                adjusted_cost,
                provider,
                product,
                report_adjustment_policy_id,
            )

            self.cost_report_data_mgr.create_cost_report_data(cost_report_data)

    def _create_cost_report_data_for_all_methods(
        self, adjustments: list, report_adjustment_policy_id: str
    ) -> None:
        currency_map, _ = self.currency_mgr.get_currency_map_date(
            currency_end_date=datetime.utcnow()
        )
        total_adjusted_cost = {}
        unified_cost_list = None

        for adjustment in adjustments:
            value = adjustment.get("value")
            method = adjustment.get("method")
            provider = adjustment.get("provider")
            product = adjustment.get("name")
            currency = adjustment.get("currency")

            # Not implemented
            filters = adjustment.get("adjustment_filter")

            if method == "FIXED":
                adjusted_cost = self._convert_fixed_value_to_adjusted_cost(
                    value, currency, currency_map, total_adjusted_cost
                )
                cost_report_data = self._build_cost_report_data_dict(
                    adjusted_cost,
                    provider,
                    product,
                    report_adjustment_policy_id,
                )
                self.cost_report_data_mgr.create_cost_report_data(cost_report_data)

            elif method == "PERCENTAGE":
                if unified_cost_list is None:
                    project_ids = None
                    if not self.project_id:
                        project_ids = [self.project_id]
                    unified_cost_list = (
                        self.unified_cost_mgr.analyze_unified_cost_for_report(
                            self.report_month,
                            self.data_source_ids,
                            self.domain_id,
                            [self.workspace_id],
                            project_ids,
                            scope="WORKSPACE",
                        )
                    )
                for unified_cost in unified_cost_list:
                    adjusted_cost = self._calculate_percentage_adjustment_cost(
                        unified_cost, value, total_adjusted_cost
                    )
                    data_source_id = unified_cost["data_source_id"]
                    cost_report_data = self._build_cost_report_data_dict(
                        adjusted_cost,
                        provider,
                        product,
                        report_adjustment_policy_id,
                        data_source_id,
                    )
                    self.cost_report_data_mgr.create_cost_report_data(cost_report_data)

    def _convert_fixed_value_to_adjusted_cost(
        self, value, currency, currency_map, total_adjusted_cost
    ):
        adjusted_cost = {}
        fixed_value = self.unified_cost_mgr.get_exchange_currency(
            value, currency, currency_map
        )
        for cur, val in fixed_value.items():
            adjusted_cost[cur] = adjusted_cost.get(cur, 0) + float(val)
            total_adjusted_cost[cur] = total_adjusted_cost.get(cur, 0) + float(val)
        return adjusted_cost

    def _calculate_percentage_adjustment_cost(
        self, unified_cost, percentage, total_adjusted_cost
    ):
        adjusted_cost = self._extract_cost_by_currency(
            {f"cost_{cur}": unified_cost.get(f"cost_{cur}", 0) for cur in CURRENCIES}
        )

        for cur in CURRENCIES:
            adjusted_cost[cur] += total_adjusted_cost.get(cur, 0)
            adjusted_cost[cur] = adjusted_cost[cur] * (percentage / 100)
            total_adjusted_cost[cur] = (
                total_adjusted_cost.get(cur, 0) + adjusted_cost[cur]
            )
        return adjusted_cost

    def _build_cost_report_data_dict(
        self,
        adjusted_cost,
        provider,
        product,
        report_adjustment_policy_id,
        data_source_id=None,
    ):
        return {
            "is_adjusted": True,
            "cost": adjusted_cost,
            "provider": provider,
            "product": product,
            "billed_month": self.cost_report_vo.report_month,
            "billed_year": self.cost_report_vo.report_month.split("-")[0],
            "report_month": self.cost_report_vo.report_month,
            "report_year": self.cost_report_vo.report_month.split("-")[0],
            "issue_date": self.cost_report_vo.issue_date,
            "is_confirmed": False,
            "data_source_id": data_source_id,
            "workspace_id": self.cost_report_vo.workspace_id,
            "workspace_name": self.cost_report_vo.workspace_name,
            "project_id": self.cost_report_vo.project_id,
            "domain_id": self.cost_report_vo.domain_id,
            "cost_report_id": self.cost_report_vo.cost_report_id,
            "cost_report_config_id": self.cost_report_vo.cost_report_config_id,
            "report_adjustment_policy_id": report_adjustment_policy_id,
        }

    def _check_applicable_policies(self) -> bool:
        policies = self.policy_mgr.list_sorted_policies_by_order(
            cost_report_config_id=self.config_id,
            domain_id=self.domain_id,
        )
        for policy in policies:
            report_adjustment_policy_id = policy["report_adjustment_policy_id"]
            adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
                report_adjustment_policy_id, self.domain_id
            )
            if adjustments:
                return True
        return False

    @staticmethod
    def _extract_cost_by_currency(unified_cost):
        return {
            currency.replace("cost_", ""): value
            for currency, value in unified_cost.items()
            if currency.startswith("cost_")
        }

    def _is_policy_applicable(self, policy):
        policy_filter = policy.get("policy_filter", {})
        workspace_ids = policy_filter.get("workspace_ids", [])
        project_ids = policy_filter.get("project_ids", [])

        if not workspace_ids and not project_ids:
            return True

        if workspace_ids and project_ids:
            return self.workspace_id in workspace_ids and self.project_id in project_ids

        if workspace_ids and not project_ids:
            return self.workspace_id in workspace_ids

        return False
