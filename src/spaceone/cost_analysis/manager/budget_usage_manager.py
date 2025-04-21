import logging
from datetime import datetime, timezone
from typing import Tuple

from dateutil.rrule import rrule, MONTHLY

from spaceone.core import config
from spaceone.core.manager import BaseManager

from spaceone.cost_analysis.manager.email_manager import EmailManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.notification_manager import NotificationManager
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.unified_cost_manager import UnifiedCostManager
from spaceone.cost_analysis.model.budget_usage.database import BudgetUsage
from spaceone.cost_analysis.model.budget.database import Budget

_LOGGER = logging.getLogger(__name__)


class BudgetUsageManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.budget_mgr = BudgetManager()
        self.notification_mgr = NotificationManager()
        self.email_mgr = None

        self.budget_usage_model = BudgetUsage

    def create_budget_usages(self, budget_vo: Budget) -> None:
        if budget_vo.time_unit == "TOTAL":
            start_dt = datetime.strptime(budget_vo.start, "%Y-%m")
            end_dt = datetime.strptime(budget_vo.end, "%Y-%m")

            dts = [dt for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt)]

            for dt in dts:
                budget_usage_data = {
                    "budget_id": budget_vo.budget_id,
                    "name": budget_vo.name,
                    "date": dt.strftime("%Y-%m"),
                    "cost": 0,
                    "limit": 0,
                    "currency": budget_vo.currency,
                    "budget": budget_vo,
                    "resource_group": budget_vo.resource_group,
                    "service_account_id": budget_vo.service_account_id,
                    "project_id": budget_vo.project_id,
                    "workspace_id": budget_vo.workspace_id,
                    "domain_id": budget_vo.domain_id,
                }

                budget_usage_vo = self.budget_usage_model.create(budget_usage_data)

        else:
            for planned_limit in budget_vo.planned_limits:
                budget_usage_data = {
                    "budget_id": budget_vo.budget_id,
                    "name": budget_vo.name,
                    "date": planned_limit["date"],
                    "cost": 0,
                    "limit": planned_limit.limit,
                    "currency": budget_vo.currency,
                    "budget": budget_vo,
                    "resource_group": budget_vo.resource_group,
                    "service_account_id": budget_vo.service_account_id,
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

    def update_cost_usage(self, budget_vo: Budget):
        _LOGGER.info(f"[update_cost_usage] Update Budget Usage: {budget_vo.budget_id}")
        unified_cost_mgr = UnifiedCostManager()

        self._update_monthly_budget_usage(unified_cost_mgr, budget_vo)

    def update_budget_usage(
        self, domain_id: str, workspace_id: str, budget_month: str = None
    ) -> None:

        query_filter = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_id, "o": "eq"},
            ]
        }

        if budget_month:
            query_filter["filter"].extend(
                [
                    {"k": "end", "v": budget_month, "o": "gte"},
                    {"k": "start", "v": budget_month, "o": "gte"},
                ]
            )

        budget_vos, _ = self.budget_mgr.list_budgets(query_filter)

        for budget_vo in budget_vos:
            self.update_cost_usage(budget_vo)
            self.notify_budget_usage(budget_vo)

    def notify_budget_usage(self, budget_vo: Budget) -> None:
        budget_id = budget_vo.budget_id
        notification = budget_vo.notification

        plans = notification.plans or []
        updated_plans = []
        is_changed = False
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")

        if current_month > budget_vo.end:
            _LOGGER.debug(
                f"[notify_budget_usage] skip notification: budget is expired ({budget_id})"
            )
            return

        for plan in plans:

            if plan.notified:
                _LOGGER.debug(
                    f"[notify_budget_usage] skip notification: already notified {budget_id} (usage percent: {budget_vo.utilization_rate}%, threshold: {plan.threshold}%)"
                )
                continue

            plan_info = plan.to_dict()
            unit = plan_info["unit"]
            threshold = plan_info["threshold"]

            total_budget_usage, budget_limit = self._get_budget_usage_and_limit(
                budget_vo, current_month
            )

            if budget_limit == 0:
                _LOGGER.debug(f"[notify_budget_usage] budget_limit is 0: {budget_id}")
                continue

            is_notify = False
            budget_percentage = budget_vo.utilization_rate

            if unit == "PERCENT":
                if budget_vo.utilization_rate > threshold:
                    is_notify = True
                    is_changed = True

            if is_notify:
                _LOGGER.debug(
                    f"[notify_budget_usage] notify event: {budget_id}, current month: {current_month} (plan: {plan.to_dict()})"
                )
                try:
                    self._notify_message(
                        budget_vo,
                        total_budget_usage,
                        budget_percentage,
                        threshold,
                    )
                    plan_info["notified"] = True

                except Exception as e:
                    _LOGGER.error(
                        f"[notify_budget_usage] Failed to notify message ({budget_id}): plan: {plan_info}, {e}",
                        exc_info=True,
                    )
                    plan_info["notified"] = False
                finally:
                    updated_plans.append(plan_info)
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

                updated_plans.append(plan_info)

        if is_changed:
            notification.plans = updated_plans
            budget_vo.update({"notification": notification.to_dict()})

    def delete_budget_usage_by_budget_vo(self, budget_vo: Budget) -> None:
        budget_usage_vos = self.filter_budget_usages(
            budget_id=budget_vo.budget_id, domain_id=budget_vo.domain_id
        )
        budget_usage_vos.delete()

    def _notify_message(
        self,
        budget_vo: Budget,
        total_budget_usage: float,
        budget_percentage: float,
        threshold: int,
    ) -> None:

        if not self.email_mgr:
            self.email_mgr = EmailManager()

        identity_mgr = IdentityManager()
        today_date = datetime.now().strftime("%Y-%m-%d")

        workspace_name = identity_mgr.get_workspace(
            budget_vo.workspace_id, budget_vo.domain_id
        )

        if budget_vo.service_account_id:
            service_account_info = identity_mgr.get_service_account(
                budget_vo.service_account_id,
                budget_vo.domain_id,
                budget_vo.workspace_id,
            )

            target_name = service_account_info.get("name")
        else:
            project_info = identity_mgr.get_project(
                budget_vo.project_id, budget_vo.domain_id
            )
            target_name = project_info.get("name")

        user_info_map = self._get_user_info_map_from_recipients(identity_mgr, budget_vo)

        console_link = self._get_console_budget_url(
            budget_vo.domain_id,
            budget_vo.workspace_id,
            budget_vo.budget_id,
        )

        for user_id, user_info in user_info_map.items():
            try:
                email = user_info["email"]
                language = user_info.get("language", "en")

                self.email_mgr.send_budget_usage_alert_email(
                    email=email,
                    language=language,
                    user_id=user_id,
                    threshold=threshold,
                    total_budget_usage=total_budget_usage,
                    budget_percentage=budget_percentage,
                    today_date=today_date,
                    workspace_name=workspace_name,
                    target_name=target_name,
                    budget_vo=budget_vo,
                    console_link=console_link,
                )
            except Exception as e:
                _LOGGER.error(
                    f"[_notify_message] Failed to send email: {user_id}, {user_info}, ({budget_vo.budget_id}): {e}"
                )

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

    def _update_monthly_budget_usage(
        self,
        unified_cost_mgr: UnifiedCostManager,
        budget_vo: Budget,
    ) -> None:

        budget_usage_by_month_map = self._get_update_budget_usage_map_from_unified_cost(
            unified_cost_mgr, budget_vo
        )

        total_usage_cost = 0
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        budget_usage_vos = self.budget_usage_model.filter(budget_id=budget_vo.budget_id)

        for budget_usage_vo in budget_usage_vos:
            if budget_usage_vo.date in budget_usage_by_month_map:
                total_usage_cost += budget_usage_by_month_map[budget_usage_vo.date]
                budget_usage_vo.update(
                    {
                        "cost": budget_usage_by_month_map[budget_usage_vo.date],
                    }
                )
            else:
                budget_usage_vo.update({"cost": 0})

        if budget_vo.time_unit == "TOTAL":
            budget_utilization_rate = round(total_usage_cost / budget_vo.limit * 100, 2)
            self.budget_mgr.update_budget_by_vo(
                {"utilization_rate": budget_utilization_rate}, budget_vo
            )
        else:
            for budget_usage_vo in budget_usage_vos:
                if budget_usage_vo.date == current_month:
                    budget_utilization_rate = round(
                        budget_usage_vo.cost / budget_usage_vo.limit * 100, 2
                    )
                    self.budget_mgr.update_budget_by_vo(
                        {"utilization_rate": budget_utilization_rate}, budget_vo
                    )
                    break

    @staticmethod
    def _get_user_info_map_from_recipients(
        identity_mgr: IdentityManager,
        budget_vo: Budget,
    ) -> dict:
        user_info_map = {}

        domain_id = budget_vo.domain_id
        workspace_id = budget_vo.workspace_id
        project_id = budget_vo.project_id
        service_account_id = budget_vo.service_account_id
        recipients = budget_vo.notification.recipients.to_dict()

        user_ids = recipients.get("users", [])
        role_types = recipients.get("role_types", [])
        service_account_manager = recipients.get("service_account_manager", "DISABLED")
        budget_manager_notification = recipients.get(
            "budget_manager_notification", "ENABLED"
        )

        if budget_manager_notification == "ENABLED":
            budget_manager_id = budget_vo.budget_manager_id
            if budget_manager_id:
                user_ids.append(budget_manager_id)

        if service_account_manager == "ENABLED":
            service_accounts_info = []
            if service_account_id:
                service_account_info = identity_mgr.get_service_account(
                    service_account_id, domain_id, workspace_id
                )
                service_accounts_info.append(service_account_info)

            else:
                query_filter = {
                    "filter": [
                        {"k": "domain_id", "v": domain_id, "o": "eq"},
                        {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                        {"k": "project_id", "v": project_id, "o": "eq"},
                    ]
                }
                response = identity_mgr.list_service_accounts(query_filter, domain_id)
                service_accounts_info = response.get("results", [])

            for service_account_info in service_accounts_info:
                if service_account_mgr_id := service_account_info.get(
                    "service_account_mgr_id"
                ):
                    user_ids.append(service_account_mgr_id)

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "workspace_id", "v": workspace_id, "o": "eq"},
            ],
            "filter_or": [],
        }

        if user_ids:
            user_ids = list(set(user_ids))
            query["filter_or"].append({"k": "user_id", "v": user_ids, "o": "in"})

        if role_types:
            query["filter_or"].append({"k": "role_type", "v": role_types, "o": "in"})

        _LOGGER.debug(f"[_get_user_info_map_from_recipients] query: {query}")

        response = identity_mgr.list_role_bindings({"query": query}, domain_id)

        users = [rb_info.get("user_id") for rb_info in response.get("results", [])]
        rb_total_count = response.get("total_count", 0)

        _LOGGER.debug(
            f"[_get_user_info_map_from_recipients] total role bindings count: {rb_total_count}"
        )

        if users:
            response = identity_mgr.list_email_verified_users(domain_id, users)
            users_info = response.get("results", [])
            user_total_count = response.get("total_count", 0)

            for user_info in users_info:
                user_info_map[user_info["user_id"]] = {
                    "email": user_info["email"],
                    "language": user_info.get("language", "en"),
                }

            _LOGGER.debug(
                f"[_get_user_info_map_from_recipients] total users: {user_total_count}"
            )

        return user_info_map

    def _get_console_budget_url(
        self, domain_id: str, workspace_id: str, budget_id: str
    ) -> str:
        domain_name = self._get_domain_name(domain_id)

        console_domain = config.get_global("EMAIL_CONSOLE_DOMAIN")
        console_domain = console_domain.format(domain_name=domain_name)

        return f"{console_domain}/workspace/{workspace_id}/cost-explorer/budget/{budget_id}"

    def _get_domain_name(self, domain_id: str) -> str:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        domain_name = identity_mgr.get_domain_name(domain_id)
        return domain_name

    @staticmethod
    def _make_unified_cost_analyze_query(budget_vo: Budget):
        query = {
            "granularity": "MONTHLY",
            "start": budget_vo.start,
            "end": budget_vo.end,
            "fields": {
                "cost": {"key": f"cost.{budget_vo.currency}", "operator": "sum"}
            },
            "filter": [
                {"k": "domain_id", "v": budget_vo.domain_id, "o": "eq"},
            ],
        }

        if budget_vo.workspace_id:
            query["filter"].append(
                {"k": "workspace_id", "v": budget_vo.workspace_id, "o": "eq"}
            )

        if budget_vo.service_account_id:
            query["filter"].append(
                {
                    "k": "service_account_id",
                    "v": budget_vo.service_account_id,
                    "o": "eq",
                }
            )
        elif budget_vo.project_id:
            query["filter"].append(
                {"k": "project_id", "v": budget_vo.project_id, "o": "eq"}
            )
        return query

    def _get_budget_usage_and_limit(
        self, budget_vo: Budget, current_month: str
    ) -> Tuple[float, float]:
        total_budget_usage = 0
        budget_limit = 0

        budget_id = budget_vo.budget_id
        domain_id = budget_vo.domain_id
        workspace_id = budget_vo.workspace_id

        if budget_vo.time_unit == "TOTAL":

            budget_usage_vos = self.filter_budget_usages(
                budget_id=budget_id,
                workspace_id=workspace_id,
                domain_id=domain_id,
            )
            total_budget_usage = sum([bs_vo.cost for bs_vo in budget_usage_vos])
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
            else:
                total_budget_usage = budget_usage_vos[0].cost
                budget_limit = budget_usage_vos[0].limit

        return round(total_budget_usage, 2), budget_limit

    def _get_update_budget_usage_map_from_unified_cost(
        self, unified_cost_mgr: UnifiedCostManager, budget_vo: Budget
    ) -> dict:
        budget_usage_by_month_map = {}
        query = self._make_unified_cost_analyze_query(budget_vo=budget_vo)
        _LOGGER.debug(f"[_update_monthly_budget_usage]: query: {query}")

        result = unified_cost_mgr.analyze_unified_costs_by_granularity(
            query, budget_vo.domain_id
        )

        for unified_cost_usage_data in result.get("results", []):
            if date := unified_cost_usage_data.get("date"):
                budget_usage_by_month_map[date] = unified_cost_usage_data.get("cost", 0)
        return budget_usage_by_month_map
