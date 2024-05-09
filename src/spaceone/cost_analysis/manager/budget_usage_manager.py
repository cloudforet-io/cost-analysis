import logging
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from spaceone.core.manager import BaseManager
from spaceone.core import utils
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.notification_manager import NotificationManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.model.budget_usage_model import BudgetUsage
from spaceone.cost_analysis.model.budget_model import Budget

_LOGGER = logging.getLogger(__name__)


class BudgetUsageManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_mgr: BudgetManager = self.locator.get_manager("BudgetManager")
        self.budget_usage_model: BudgetUsage = self.locator.get_model("BudgetUsage")
        self.notification_mgr: NotificationManager = self.locator.get_manager(
            "NotificationManager"
        )
        self.data_source_mgr: DataSourceManager = self.locator.get_manager(
            "DataSourceManager"
        )

    def create_budget_usages(self, budget_vo: Budget) -> None:
        if budget_vo.time_unit == "TOTAL":
            start_dt = datetime.strptime(budget_vo.start, "%Y-%m")
            end_dt = datetime.strptime(budget_vo.end, "%Y-%m")

            dts = [dt for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt)]
            limit_per_month = round(budget_vo.limit / len(dts), 3)
            budget_limit = budget_vo.limit

            for dt in dts:
                if budget_limit - limit_per_month < 0:
                    limit_per_month = round(budget_limit, 3)
                budget_usage_data = {
                    "budget_id": budget_vo.budget_id,
                    "name": budget_vo.name,
                    "date": dt.strftime("%Y-%m"),
                    "cost": 0,
                    "limit": limit_per_month,
                    "currency": budget_vo.currency,
                    "provider_filter": budget_vo.provider_filter.to_dict(),
                    "budget": budget_vo,
                    "resource_group": budget_vo.resource_group,
                    "data_source_id": budget_vo.data_source_id,
                    "project_id": budget_vo.project_id,
                    "workspace_id": budget_vo.workspace_id,
                    "domain_id": budget_vo.domain_id,
                }

                budget_usage_vo = self.budget_usage_model.create(budget_usage_data)
                budget_limit -= limit_per_month

        else:
            for planned_limit in budget_vo.planned_limits:
                budget_usage_data = {
                    "budget_id": budget_vo.budget_id,
                    "name": budget_vo.name,
                    "date": planned_limit["date"],
                    "cost": 0,
                    "limit": planned_limit.limit,
                    "currency": budget_vo.currency,
                    "provider_filter": budget_vo.provider_filter.to_dict(),
                    "budget": budget_vo,
                    "resource_group": budget_vo.resource_group,
                    "data_source_id": budget_vo.data_source_id,
                    "project_id": budget_vo.project_id,
                    "workspace_id": budget_vo.workspace_id,
                    "domain_id": budget_vo.domain_id,
                }

                budget_usage_vo = self.budget_usage_model.create(budget_usage_data)

    def update_budget_usage_by_vo(self, params, budget_usage_vo):
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_budget_usage_by_vo._rollback] Revert Data : "
                f'{old_data["budget_id"]} / {old_data["date"]}'
            )
            budget_usage_vo.update(old_data)

        self.transaction.add_rollback(_rollback, budget_usage_vo.to_dict())
        return budget_usage_vo.update(params)

    def update_cost_usage(
        self,
        budget_vo: Budget,
    ):
        _LOGGER.info(f"[update_cost_usage] Update Budget Usage: {budget_vo.budget_id}")
        cost_mgr: CostManager = self.locator.get_manager("CostManager")

        self._update_monthly_budget_usage(budget_vo, cost_mgr)

    def update_budget_usage(self, domain_id: str, data_source_id: str):
        budget_vos = self.budget_mgr.filter_budgets(
            domain_id=domain_id,
            data_source_id=data_source_id,
        )
        for budget_vo in budget_vos:
            self.update_cost_usage(budget_vo)
            self.notify_budget_usage(budget_vo)

    def notify_budget_usage(self, budget_vo: Budget):
        budget_id = budget_vo.budget_id
        workspace_id = budget_vo.workspace_id
        domain_id = budget_vo.domain_id
        current_month = datetime.utcnow().strftime("%Y-%m")
        updated_notifications = []
        is_changed = False
        for notification in budget_vo.notifications:
            if current_month not in notification.notified_months:
                unit = notification.unit
                threshold = notification.threshold
                notification_type = notification.notification_type
                is_notify = False

                if budget_vo.time_unit == "TOTAL":
                    budget_usage_vos = self.filter_budget_usages(
                        budget_id=budget_id,
                        workspace_id=workspace_id,
                        domain_id=domain_id,
                    )
                    total_budget_usage = sum(
                        [budget_usage_vo.cost for budget_usage_vo in budget_usage_vos]
                    )
                    budget_limit = budget_vo.limit
                else:
                    budget_usage_vos = self.filter_budget_usages(
                        budget_id=budget_id,
                        workspace_id=workspace_id,
                        domain_id=domain_id,
                        date=current_month,
                    )

                    if budget_usage_vos.count() == 0:
                        _LOGGER.debug(
                            f"[notify_budget_usage] budget_usage_vos is empty: {budget_id}"
                        )
                        continue

                    total_budget_usage = budget_usage_vos[0].cost
                    budget_limit = budget_usage_vos[0].limit

                if budget_limit == 0:
                    _LOGGER.debug(
                        f"[notify_budget_usage] budget_limit is 0: {budget_id}"
                    )
                    continue

                budget_percentage = round(total_budget_usage / budget_limit * 100, 2)

                if unit == "PERCENT":
                    if budget_percentage > threshold:
                        is_notify = True
                        is_changed = True
                else:
                    if total_budget_usage > threshold:
                        is_notify = True
                        is_changed = True

                if is_notify:
                    _LOGGER.debug(
                        f"[notify_budget_usage] notify event: {budget_id} (level: {notification_type})"
                    )
                    try:
                        self._notify_message(
                            budget_vo,
                            current_month,
                            total_budget_usage,
                            budget_limit,
                            budget_percentage,
                            threshold,
                            unit,
                            notification_type,
                        )

                        updated_notifications.append(
                            {
                                "threshold": threshold,
                                "unit": unit,
                                "notification_type": notification_type,
                                "notified_months": notification.notified_months
                                + [current_month],
                            }
                        )
                    except Exception as e:
                        _LOGGER.error(
                            f"[notify_budget_usage] Failed to notify message ({budget_id}): {e}"
                        )
                else:
                    if unit == "PERCENT":
                        _LOGGER.debug(
                            f"[notify_budget_usage] skip notification: {budget_id} "
                            f"(usage percent: {budget_percentage}%, threshold: {threshold}%)"
                        )
                    else:
                        _LOGGER.debug(
                            f"[notify_budget_usage] skip notification: {budget_id} "
                            f"(usage cost: {total_budget_usage}, threshold: {threshold})"
                        )

                    updated_notifications.append(notification.to_dict())

            else:
                updated_notifications.append(notification.to_dict())

        if is_changed:
            budget_vo.update({"notifications": updated_notifications})

    def _notify_message(
        self,
        budget_vo: Budget,
        current_month,
        total_budget_usage,
        budget_limit,
        budget_percentage,
        threshold,
        unit,
        notification_type,
    ):
        data_source_name = self.data_source_mgr.get_data_source(
            budget_vo.data_source_id, budget_vo.domain_id
        ).name
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        project_name = identity_mgr.get_project_name(
            budget_vo.project_id, budget_vo.workspace_id, budget_vo.domain_id
        )
        workspace_name = identity_mgr.get_workspace(
            budget_vo.workspace_id, budget_vo.domain_id
        )

        if unit == "PERCENT":
            threshold_str = f"{int(threshold)}%"
        else:
            threshold_str = format(int(threshold), ",")

        description = f"Please check the budget usage and increase the budget limit if necessary.\n\n"
        description += (
            f"Budget Usage (Currency: {budget_vo.currency}): \n"
            f'- Usage Cost: {format(round(total_budget_usage, 2), ",")}\n'
            f'- Limit: {format(budget_limit, ",")}\n'
            f"- Percentage: {budget_percentage}%\n"
            f"- Threshold: > {threshold_str}\n"
        )

        if budget_vo.time_unit == "MONTHLY":
            period = f"{current_month} ~ {current_month}"
        else:
            period = f"{budget_vo.start} ~ {budget_vo.end}"

        message = {
            "resource_type": "identity.Project",
            "resource_id": budget_vo.project_id,
            "notification_type": "WARNING"
            if notification_type == "WARNING"
            else "ERROR",
            "topic": "cost_analysis.Budget",
            "message": {
                "title": f"Budget usage exceeded - {budget_vo.name}",
                "description": description,
                "tags": [
                    {
                        "key": "Budget ID",
                        "value": budget_vo.budget_id,
                        "options": {"short": True},
                    },
                    {
                        "key": "Budget Name",
                        "value": budget_vo.name,
                        "options": {"short": True},
                    },
                    {
                        "key": "Data Source",
                        "value": data_source_name,
                        "options": {"short": True},
                    },
                    {"key": "Period", "value": period, "options": {"short": True}},
                    {
                        "key": "Project",
                        "value": project_name,
                    },
                    {
                        "key": "Workspace",
                        "value": workspace_name,
                    },
                ],
                "occurred_at": utils.datetime_to_iso8601(datetime.utcnow()),
            },
            "notification_level": "ALL",
            "workspace_id": budget_vo.workspace_id,
            "domain_id": budget_vo.domain_id,
        }

        self.notification_mgr.create_notification(message)

    def filter_budget_usages(self, **conditions):
        return self.budget_usage_model.filter(**conditions)

    def list_budget_usages(self, query={}):
        return self.budget_usage_model.query(**query)

    def stat_budget_usages(self, query):
        return self.budget_usage_model.stat(**query)

    def analyze_budget_usages(self, query):
        query["date_field"] = "date"
        query["date_field_format"] = "%Y-%m"
        return self.budget_usage_model.analyze(**query)

    def _update_monthly_budget_usage(self, budget_vo: Budget, cost_mgr: CostManager):
        update_data = {}
        query = self._make_cost_analyze_query(budget_vo=budget_vo)
        _LOGGER.debug(f"[_update_monthly_budget_usage]: query: {query}")

        result = cost_mgr.analyze_costs_by_granularity(
            query, budget_vo.domain_id, budget_vo.data_source_id
        )

        for cost_usage_data in result.get("results", []):
            if date := cost_usage_data.get("date"):
                update_data[date] = cost_usage_data.get("cost", 0)

        budget_usage_vos = self.budget_usage_model.filter(budget_id=budget_vo.budget_id)
        for budget_usage_vo in budget_usage_vos:
            if budget_usage_vo.date in update_data:
                budget_usage_vo.update({"cost": update_data[budget_usage_vo.date]})
            else:
                budget_usage_vo.update({"cost": 0})

    @staticmethod
    def _make_cost_analyze_query(budget_vo: Budget):
        query = {
            "granularity": "MONTHLY",
            "start": budget_vo.start,
            "end": budget_vo.end,
            "fields": {"cost": {"key": "cost", "operator": "sum"}},
            "filter": [
                {"k": "domain_id", "v": budget_vo.domain_id, "o": "eq"},
                {"k": "workspace_id", "v": budget_vo.workspace_id, "o": "eq"},
                {"k": "data_source_id", "v": budget_vo.data_source_id, "o": "eq"},
            ],
        }

        if budget_vo.project_id and budget_vo.project_id != "*":
            query["filter"].append(
                {"k": "project_id", "v": budget_vo.project_id, "o": "eq"}
            )

        if budget_vo.provider_filter and budget_vo.provider_filter.state == "ENABLED":
            query["filter"].append(
                {"k": "provider", "v": budget_vo.provider_filter.providers, "o": "in"}
            )

        return query
