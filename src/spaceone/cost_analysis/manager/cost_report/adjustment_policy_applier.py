import logging
from datetime import datetime

from spaceone.core import config

from spaceone.cost_analysis.model.report_adjustment_policy.database import (
    ReportAdjustmentPolicy,
)
from spaceone.cost_analysis.manager import (
    ReportAdjustmentManager,
    ReportAdjustmentPolicyManager,
    CostReportDataManager,
    UnifiedCostManager,
)
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.model import CostReport

_LOGGER = logging.getLogger(__name__)


class AdjustmentPolicyApplier:
    def __init__(self, cost_report_vo: CostReport, data_source_ids):
        self.policy_mgr = ReportAdjustmentPolicyManager()
        self.adjustment_mgr = ReportAdjustmentManager()
        self.cost_report_data_mgr = CostReportDataManager()
        self.unified_cost_mgr = UnifiedCostManager()
        self.currency_mgr = CurrencyManager()

        self.cost_report_vo = cost_report_vo

        self.config_id = cost_report_vo.cost_report_config_id
        self.report_month = cost_report_vo.report_month
        self.currency_date = datetime.strptime(cost_report_vo.currency_date, "%Y-%m-%d")
        self.issue_date = cost_report_vo.issue_date
        self.domain_id = cost_report_vo.domain_id
        self.name = cost_report_vo.name
        self.workspace_id = cost_report_vo.workspace_id
        self.project_id = cost_report_vo.project_id

        self.data_source_ids = data_source_ids

        self.policies = []
        self.is_applied = self._check_applicable_policies()

        self.currencies = config.get_global(
            "SUPPORTED_CURRENCIES", ["KRW", "USD", "JPY"]
        )

    def apply_policies(self) -> None:
        policies = self.policy_mgr.list_sorted_policies_by_order(
            cost_report_config_id=self.config_id,
            domain_id=self.domain_id,
        )

        for policy in policies:
            if not self._is_policy_applicable(policy):
                continue

            _LOGGER.debug(f"Applying policy: {policy['report_adjustment_policy_id']}")

            report_adjustment_policy_id = policy["report_adjustment_policy_id"]
            adjustments = self.adjustment_mgr.list_sorted_adjustments_by_order(
                report_adjustment_policy_id, self.domain_id
            )

            if not adjustments:
                continue

            self._create_cost_report_data_for_all_methods(
                adjustments, report_adjustment_policy_id
            )

            self.is_applied = True

    def _create_cost_report_data_for_all_methods(
        self, adjustments: list, report_adjustment_policy_id: str
    ):
        currency_map, _ = self.currency_mgr.get_currency_map_date(
            currency_end_date=self.currency_date
        )

        adjustment_policy_vo = self.policy_mgr.get_policy(
            policy_id=report_adjustment_policy_id, domain_id=self.domain_id
        )

        for adjustment_info in adjustments:
            query_filter = self._make_query_filter_with_adjustment(
                adjustment_policy_vo, adjustment_info
            )
            value = adjustment_info.get("value")
            unit = adjustment_info.get("unit")
            currency = adjustment_info.get("currency")
            provider = adjustment_info.get("provider")
            product = adjustment_info.get("name")
            description = adjustment_info.get("description")

            adjusted_cost = self._calculate_percentage_adjustment_cost(
                query_filter, unit, value, currency
            )
            if adjusted_cost:
                cost_report_data = self._build_cost_report_data_dict(
                    adjusted_cost,
                    provider,
                    product,
                    report_adjustment_policy_id,
                    description,
                )

                self.cost_report_data_mgr.create_cost_report_data(cost_report_data)

    def _convert_fixed_value_to_adjusted_cost(self, value, currency, currency_map):
        adjusted_cost = {}
        fixed_value = self.unified_cost_mgr.get_exchange_currency(
            value, currency, currency_map
        )
        for cur, val in fixed_value.items():
            adjusted_cost[cur] = adjusted_cost.get(cur, 0) + float(val)
        return adjusted_cost

    def _calculate_percentage_adjustment_cost(
        self, query_filter: dict, unit: str, value: float, currency: str
    ):
        adjusted_cost = {}

        if unit == "PERCENT":
            response = self.cost_report_data_mgr.analyze_cost_reports_data(query_filter)
            results = response.get("results", [])

            if results:
                for cur in self.currencies:
                    adjusted_cost[cur] = results[0][cur] * (value / 100)
        elif unit == "FIXED":
            currency_map, _ = self.currency_mgr.get_currency_map_date(
                currency_end_date=self.currency_date,
            )
            fixed_value = self.unified_cost_mgr.get_exchange_currency(
                value, currency, currency_map
            )

            for cur, val in fixed_value.items():
                adjusted_cost[cur] = adjusted_cost.get(cur, 0) + float(val)

        return adjusted_cost

    def _build_cost_report_data_dict(
        self,
        adjusted_cost: dict,
        provider: str,
        product: str,
        report_adjustment_policy_id: str,
        description: str,
    ):
        return {
            "is_adjusted": True,
            "cost": adjusted_cost,
            "usage_type": description,
            "provider": provider,
            "product": product,
            "billed_month": self.cost_report_vo.report_month,
            "billed_year": self.cost_report_vo.report_month.split("-")[0],
            "report_month": self.cost_report_vo.report_month,
            "report_year": self.cost_report_vo.report_month.split("-")[0],
            "issue_date": self.cost_report_vo.issue_date,
            "is_confirmed": False,
            "workspace_id": self.cost_report_vo.workspace_id,
            "name": self.cost_report_vo.name,
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

    def _make_query_filter_with_adjustment(
        self, adjustment_policy_vo: ReportAdjustmentPolicy, adjustment_info: dict
    ) -> dict:
        provider = adjustment_info.get("provider")

        query_filter = {
            "filter": [
                {"k": "domain_id", "v": self.domain_id, "o": "eq"},
                {"k": "cost_report_config_id", "v": self.config_id, "o": "eq"},
                {"k": "report_month", "v": self.report_month, "o": "eq"},
                {
                    "k": "report_adjustment_policy_id",
                    "v": adjustment_info["report_adjustment_policy_id"],
                    "o": "not",
                },
            ],
            "fields": {
                "cost": {
                    "key": "cost",
                    "operator": "sum",
                },
            },
        }

        # step 1 apply adjustment policy filter
        if policy_filter := adjustment_policy_vo.policy_filter:
            if adjustment_policy_vo.scope == "WORKSPACE":
                workspace_ids = policy_filter.get("workspace_ids", [])
                if workspace_ids:
                    query_filter["filter"].append(
                        {"k": "workspace_id", "v": workspace_ids, "o": "in"}
                    )

            elif adjustment_policy_vo.scope == "PROJECT":
                project_ids = policy_filter.get("project_ids", [])
                if project_ids:
                    query_filter["filter"].append(
                        {"k": "project_id", "v": project_ids, "o": "in"}
                    )

        # step2 apply adjustment filter
        if adj_filter := adjustment_info.get("adjustment_filter", []):
            query_filter["filter"].extend(adj_filter)

        if provider:
            query_filter["filter"].append({"k": "provider", "v": provider, "o": "eq"})

        # step3 fields
        currencies = self.currencies
        fields = {
            f"{currency}": {"key": f"cost.{currency}", "operator": "sum"}
            for currency in currencies
        }
        query_filter["fields"].update(fields)

        _LOGGER.debug(
            f"[make_query_filter_with_adjustment] query_filter: {query_filter}"
        )

        return query_filter
