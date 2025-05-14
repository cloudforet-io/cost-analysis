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
        self.v_workspace_id_map = metadata["v_workspace_id_map"]
        self.workspace_name_map = metadata["workspace_name_map"]
        self.project_name_map = metadata["project_name_map"]

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
        unified_cost = unified_cost.copy()
        workspace_id = unified_cost["workspace_id"]
        unified_cost["workspace_id"] = self.v_workspace_id_map.get(
            workspace_id, workspace_id
        )
        unified_cost["cost"] = self._extract_cost_by_currency(unified_cost)
        unified_cost["status"] = status
        unified_cost["issue_date"] = (
            f"{self.report_month}-{str(self.issue_day).zfill(2)}"
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

        if self.scope == "PROJECT":
            project_id = unified_cost.get("project_id")
            unified_cost["project_name"] = self.project_name_map.get(
                project_id, "Unknown"
            )

        return unified_cost

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
