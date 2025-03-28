import logging
from datetime import datetime, timezone
from typing import Union

from dateutil.rrule import rrule, MONTHLY

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core import utils

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
        self.budget_usage_model: BudgetUsage = self.locator.get_model("BudgetUsage")
        self.notification_mgr: NotificationManager = self.locator.get_manager(
            "NotificationManager"
        )
        self.email_mgr = None

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
                    "budget": budget_vo,
                    "resource_group": budget_vo.resource_group,
                    "service_account_id": budget_vo.service_account_id,
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

    def update_cost_usage(
        self,
        budget_vo: Budget,
    ):
        _LOGGER.info(f"[update_cost_usage] Update Budget Usage: {budget_vo.budget_id}")
        unified_cost_mgr = UnifiedCostManager()

        self._update_monthly_budget_usage(budget_vo, unified_cost_mgr)

    def update_budget_usage(self, domain_id: str, workspace_id: str) -> None:
        budget_vos = self.budget_mgr.filter_budgets(
            domain_id=domain_id,
            workspace_id=workspace_id,
        )
        for budget_vo in budget_vos:
            self.update_cost_usage(budget_vo)
            self.notify_budget_usage(budget_vo)

    def notify_budget_usage(self, budget_vo: Budget) -> None:
        budget_id = budget_vo.budget_id
        workspace_id = budget_vo.workspace_id
        domain_id = budget_vo.domain_id
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        updated_plans = []
        is_changed = False
        notifications = budget_vo.notifications

        plans = notifications.plans or []

        for plan in plans:
            if current_month not in plan.notified_months:
                unit = plan.unit
                threshold = plan.threshold
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

                        updated_plans.append(
                            {
                                "threshold": threshold,
                                "unit": unit,
                                "notified_months": plan.notified_months
                                + [current_month],
                            }
                        )
                    except Exception as e:
                        _LOGGER.error(
                            f"[notify_budget_usage] Failed to notify message ({budget_id}): {e}",
                            exc_info=True,
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

                    updated_plans.append(plan.to_dict())

            else:
                updated_plans.append(plan.to_dict())

        if is_changed:
            notifications.plans = updated_plans
            budget_vo.update({"notifications": notifications.to_dict()})

    def _notify_message(
        self,
        budget_vo: Budget,
        total_budget_usage: float,
        budget_percentage,
        threshold,
    ):

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

        user_info_map = self._get_user_info_map_from_recipients(
            identity_mgr,
            budget_vo.domain_id,
            budget_vo.workspace_id,
            budget_vo.project_id,
            budget_vo.notifications.recipients.to_dict(),
            service_account_id=budget_vo.service_account_id,
        )

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
        self, budget_vo: Budget, unified_cost_mgr: UnifiedCostManager
    ) -> None:
        update_budget_usage_map = {}
        query = self._make_unified_cost_analyze_query(budget_vo=budget_vo)
        _LOGGER.debug(f"[_update_monthly_budget_usage]: query: {query}")

        result = unified_cost_mgr.analyze_unified_costs_by_granularity(
            query, budget_vo.domain_id
        )

        for unified_cost_usage_data in result.get("results", []):
            if date := unified_cost_usage_data.get("date"):
                update_budget_usage_map[date] = unified_cost_usage_data.get("cost", 0)

        budget_usage_vos = self.budget_usage_model.filter(budget_id=budget_vo.budget_id)
        for budget_usage_vo in budget_usage_vos:
            if budget_usage_vo.date in update_budget_usage_map:
                budget_usage_vo.update(
                    {"cost": update_budget_usage_map[budget_usage_vo.date]}
                )
            else:
                budget_usage_vo.update({"cost": 0})

    @staticmethod
    def _get_user_info_map_from_recipients(
        identity_mgr: IdentityManager,
        domain_id: str,
        workspace_id: str,
        project_id: str,
        recipients: dict,
        service_account_id: Union[str, None] = None,
    ) -> dict:
        user_info_map = {}

        user_ids = recipients.get("users", [])
        role_types = recipients.get("role_types", [])
        service_account_manager = recipients.get("service_account_manager", "DISABLED")

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
