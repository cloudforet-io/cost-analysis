class CostReportFormatGenerator:
    def __init__(
        self,
        issue_month: str,
        issue_day,
        v_workspace_id_map: dict,
        workspace_name_map: dict,
        report_month: str,
        cost_report_config_id: str,
        domain_id: str,
    ):
        self.issue_month = issue_month
        self.issue_day = issue_day
        self.v_workspace_id_map = v_workspace_id_map
        self.workspace_name_map = workspace_name_map
        self.report_month = report_month
        self.cost_report_config_id = cost_report_config_id
        self.domain_id = domain_id
        self.project_id = None

    def make_cost_reports(
        self,
        unified_costs: list,
        status: str,
    ) -> list:
        transformed_costs = []
        for unified_cost in unified_costs:
            unified_cost = unified_cost.copy()
            unified_cost["workspace_id"] = self.v_workspace_id_map.get(
                unified_cost["workspace_id"],
                unified_cost["workspace_id"],
            )

            unified_cost["cost"] = self._extract_cost_by_currency(unified_cost)
            unified_cost["status"] = status
            unified_cost["issue_date"] = (
                f"{self.issue_month}-{str(self.issue_day).zfill(2)}"
            )
            unified_cost["report_month"] = self.report_month
            unified_cost["report_year"] = unified_cost.pop("billed_year")
            unified_cost["workspace_name"] = self.workspace_name_map.get(
                unified_cost["workspace_id"], "Unknown"
            )
            unified_cost["bank_name"] = unified_cost.pop(
                "exchange_source", "Yahoo! Finance"
            )
            unified_cost["cost_report_config_id"] = self.cost_report_config_id
            unified_cost["domain_id"] = self.domain_id

            if self.project_id:
                unified_cost["project_id"] = self.project_id

            transformed_costs.append(unified_cost)

        cost_reports = self._aggregate_result_by_currency(transformed_costs)
        return cost_reports

    @staticmethod
    def _extract_cost_by_currency(unified_cost):
        return {
            currency.replace("cost_", ""): value
            for currency, value in unified_cost.items()
            if currency.startswith("cost_")
        }

    @staticmethod
    def _aggregate_result_by_currency(unified_costs):
        workspace_result_map = {}
        for unified_cost in unified_costs:
            workspace_id = unified_cost["workspace_id"]
            if workspace_id in workspace_result_map:
                for currency, cost in unified_cost["cost"].items():
                    workspace_result_map[workspace_id]["cost"][currency] += cost
            else:
                workspace_result_map[workspace_id] = unified_cost.copy()
        return list(workspace_result_map.values())
