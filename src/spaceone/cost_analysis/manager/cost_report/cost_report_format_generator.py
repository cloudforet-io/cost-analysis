class CostReportFormatGenerator:
    def __init__(
        self,
        metadata: dict,
        report_month: str,
        scope: str,
        cost_report_config_id: str,
        domain_id: str,
    ):
        self.issue_month = metadata["current_month"]
        self.issue_day = metadata["issue_day"]
        self.report_currency = metadata["currency"]

        self.scope = scope
        self.report_month = report_month
        self.cost_report_config_id = cost_report_config_id
        self.domain_id = domain_id

    def make_cost_reports(
        self,
        unified_costs: list,
        status: str,
    ) -> list:
        cost_reports = [
            self._transform_cost(unified_cost, status) for unified_cost in unified_costs
        ]
        return self._aggregate_result_by_currency(cost_reports)

    def _transform_cost(self, unified_cost: dict, status: str) -> dict:
        tr_unified_cost = unified_cost.copy()
        tr_unified_cost["cost"] = self._extract_cost_by_currency(tr_unified_cost)
        tr_unified_cost["status"] = status
        tr_unified_cost["issue_date"] = (
            f"{self.issue_month}-{str(self.issue_day).zfill(2)}"
        )
        tr_unified_cost["report_month"] = self.report_month
        tr_unified_cost["report_year"] = tr_unified_cost.get("billed_year")
        tr_unified_cost["bank_name"] = tr_unified_cost.get(
            "exchange_source", "Yahoo! Finance"
        )
        tr_unified_cost["cost_report_config_id"] = self.cost_report_config_id
        tr_unified_cost["domain_id"] = self.domain_id
        tr_unified_cost["currency"] = self.report_currency
        tr_unified_cost["currency_date"] = tr_unified_cost["exchange_date"]

        name = "Unknown"
        if self.scope == "WORKSPACE":
            name = tr_unified_cost.get("workspace_name", "Unknown")
        elif self.scope == "PROJECT":
            name = tr_unified_cost.get("project_name", "Unknown")
        elif self.scope == "SERVICE_ACCOUNT":
            name = tr_unified_cost.get(
                "service_account_name", "Unknown"
            )
        tr_unified_cost["name"] = name

        return tr_unified_cost

    @staticmethod
    def _extract_cost_by_currency(unified_cost):
        return {
            currency.replace("cost_", ""): value
            for currency, value in unified_cost.items()
            if currency.startswith("cost_")
        }

    @staticmethod
    def _aggregate_result_by_currency(report_costs):
        workspace_result_map = {}
        for report_cost in report_costs:
            workspace_id = report_cost["workspace_id"]
            if workspace_id in workspace_result_map:
                for currency, cost in report_cost["cost"].items():
                    workspace_result_map[workspace_id]["cost"][currency] += cost
            else:
                workspace_result_map[workspace_id] = report_cost.copy()
        return list(workspace_result_map.values())
