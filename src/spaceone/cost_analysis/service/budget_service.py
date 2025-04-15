import logging
from datetime import datetime, timezone
from dateutil.rrule import rrule, MONTHLY

from spaceone.core.service import *
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.budget.database import Budget
from spaceone.cost_analysis.model.budget.request import *
from spaceone.cost_analysis.model.budget.response import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BudgetService(BaseService):
    resource = "Budget"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_mgr = BudgetManager()
        self.budget_usage_mgr = BudgetUsageManager()

    @transaction(exclude=["authentication, authorization", "mutation"])
    def create_budget_update_job_by_domain(self, params: dict) -> None:
        """Create budget update job by domain

        Args:
            params (dict): {}

        Returns:
            None
        """

        identity_mgr = IdentityManager()
        domain_ids = identity_mgr.list_enabled_domain_ids()

        for domain_id in domain_ids:
            try:
                self.create_budget_update_job(domain_id)
            except Exception as e:
                _LOGGER.error(
                    f"[create_budget_update_job_by_domain] domain_id :{domain_id}, error: {e}",
                    exc_info=True,
                )

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def create(self, params: BudgetCreateRequest) -> Union[BudgetResponse, dict]:
        """Create budget

        Args:
            params (dict): {
                'name': 'str',              # required
                'limit': 'float',
                'planned_limits': 'list',
                'currency': 'str',          # required
                'time_unit': 'str',         # required
                'start': 'str',             # required
                'end': 'str',               # required
                'budget_year': 'str',       # required
                'notification': 'dict',
                'tags': 'dict',
                'resource_group': 'str',    # required
                'project_id': 'str',
                'workspace_id': 'str',
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            budget_vo (object)
        """

        domain_id = params.domain_id
        workspace_id = params.workspace_id
        project_id = params.project_id
        service_account_id = params.service_account_id
        budget_manager_id = params.budget_manager_id
        limit = params.limit
        planned_limits = params.planned_limits or []
        time_unit = params.time_unit
        start = params.start
        end = params.end

        notification = params.notification or {}
        resource_group = params.resource_group

        if resource_group != "PROJECT":
            raise ERROR_NOT_IMPLEMENTED()

        self._check_time_period(start, end, params.budget_year)

        identity_mgr = IdentityManager()
        identity_mgr.check_workspace(workspace_id, domain_id)
        identity_mgr.get_project(project_id, domain_id)

        if service_account_id:
            service_account_info = identity_mgr.get_service_account(
                service_account_id, domain_id, workspace_id
            )
            if service_account_info["project_id"] != project_id:
                raise ERROR_INVALID_PARAMETER(
                    key="service_account_id",
                    reason=f"{service_account_id} is not in {project_id} project",
                )

        if budget_manager_id:
            self._check_user_exists(identity_mgr, budget_manager_id, domain_id)

        if time_unit == "TOTAL":
            if limit is None:
                raise ERROR_REQUIRED_PARAMETER(key="limit")

            params.planned_limits = None

        else:
            # Check Planned Limits
            self._check_planned_limits(start, end, time_unit, planned_limits)

            params.limit = 0
            current_month = datetime.now(timezone.utc).strftime("%Y-%m")
            for planned_limit in planned_limits:
                if planned_limit["date"] == current_month:
                    params.limit = planned_limit["limit"]
                    break

        # Check Notification
        self._check_notification(notification, domain_id, workspace_id)

        # Check Duplicated Budget
        budget_vos = self.budget_mgr.filter_budgets(
            service_account_id=service_account_id,
            project_id=project_id,
            workspace_id=workspace_id,
            domain_id=domain_id,
            budget_year=params.budget_year,
        )
        if budget_vos.count() > 0:
            budget_target = params.service_account_id or params.project_id
            raise ERROR_BUDGET_ALREADY_EXIST(
                budget_year=params.budget_year,
                workspace_id=workspace_id,
                target=budget_target,
            )

        budget_vo = self.budget_mgr.create_budget(params.dict())

        # Create budget usages
        budget_usage_mgr = BudgetUsageManager()
        budget_usage_mgr.create_budget_usages(budget_vo)
        budget_usage_mgr.update_cost_usage(budget_vo)
        budget_usage_mgr.notify_budget_usage(budget_vo)

        return BudgetResponse(
            **budget_vo.to_dict(),
        )

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def update(self, params: BudgetUpdateRequest) -> Union[BudgetResponse, dict]:
        """Update budget

        Args:
            params (dict): {
                'budget_id': 'str',         # required
                'name': 'str',
                'limit': 'float',
                'planned_limits': 'list',
                'start': 'str',
                'end': 'str',
                'tags': 'dict'
                'budget_manager_id': 'str',
                'workspace_id', 'str',      # injected from auth (optional)
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            budget_vo (object)
        """

        budget_id = params.budget_id
        workspace_id = params.workspace_id
        domain_id = params.domain_id
        planned_limits = params.planned_limits or []
        budget_manager_id = params.budget_manager_id

        budget_usage_mgr = BudgetUsageManager()

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id
        )

        start = budget_vo.start
        end = budget_vo.end

        if params.start or params.end:
            start = params.start or start
            end = params.end or end
            self._check_time_period(start, end, budget_vo.budget_year)
            planned_limits = planned_limits or budget_vo.planned_limits

        # Check limit and Planned Limits
        if planned_limits:
            self._check_planned_limits(start, end, budget_vo.time_unit, planned_limits)

        if budget_manager_id:
            identity_mgr = IdentityManager()
            self._check_user_exists(identity_mgr, budget_manager_id, domain_id)

        budget_vo = self.budget_mgr.update_budget_by_vo(
            params.dict(exclude_unset=True), budget_vo
        )

        # Update Budget Usages
        if planned_limits:
            budget_usage_mgr.delete_budget_usage_by_budget_vo(budget_vo)
            budget_usage_mgr.create_budget_usages(budget_vo)
        elif "name" in params:
            budget_usage_vos = budget_usage_mgr.filter_budget_usages(
                budget_id=budget_id,
                domain_id=domain_id,
            )
            for budget_usage_vo in budget_usage_vos:
                budget_usage_mgr.update_budget_usage_by_vo(
                    {"name": params.name}, budget_usage_vo
                )

        budget_usage_mgr.update_cost_usage(budget_vo)
        budget_usage_mgr.notify_budget_usage(budget_vo)

        return BudgetResponse(**budget_vo.to_dict())

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def set_notification(
        self, params: BudgetSetNotificationRequest
    ) -> Union[BudgetResponse, dict]:
        """Set budget notification

        Args:
            params (dict): {
                'budget_id': 'str',
                'notification': 'dict',
                'workspace_id': 'str',
                'domain_id': 'str'
                'user_projects': 'list'
            }

        Returns:
            budget_vo (object)
        """
        budget_id = params.budget_id
        project_id = params.project_id
        workspace_id = params.workspace_id
        domain_id = params.domain_id
        notification = params.notification or {}

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id, project_id
        )

        # Check notification
        self._check_notification(notification, domain_id, workspace_id)
        params.notification = notification

        budget_vo = self.budget_mgr.update_budget_by_vo(
            params.dict(exclude_unset=True), budget_vo
        )
        return BudgetResponse(**budget_vo.to_dict())

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def delete(self, params: BudgetDeleteRequest) -> None:
        """Deregister budget

        Args:
            params (dict): {
                'budget_id': 'str',     # required
                'workspace_id': 'str',  # injected from auth (optional)
                'domain_id': 'str'      # injected from auth
            }

        Returns:
            None
        """

        budget_vo: Budget = self.budget_mgr.get_budget(
            params.budget_id, params.domain_id, params.workspace_id
        )
        self.budget_mgr.delete_budget_by_vo(budget_vo)

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def get(self, params: BudgetGetRequest) -> Union[BudgetResponse, dict]:
        """Get budget

        Args:
            params (dict): {
                'budget_id': 'str',         # required
                'project_id: 'str',
                'workspace_id': 'str',      # injected from auth (optional)
                'domain_id': 'str',         # injected from auth
                'user_projects': 'list'    # injected from auth (optional)
            }

        Returns:
            budget_vo (object)
        """

        budget_id = params.budget_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id
        project_id = params.project_id

        budget_vo = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id, project_id
        )

        return BudgetResponse(**budget_vo.to_dict())

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        [
            "domain_id",
            "workspace_id",
            "project_id",
            "user_projects",
            "name",
            "budget_id",
            "time_unit",
            "service_account_id",
        ]
    )
    @append_keyword_filter(["budget_id", "name", "budget_manager_id"])
    @set_query_page_limit(1000)
    @convert_model
    def list(self, params: BudgetSearchQueryRequest) -> Union[BudgetsResponse, dict]:
        """List budgets

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'budget_id': 'str',
                'name': 'str',
                'time_unit': 'str',
                'project_id': 'str',
                'user_projects': 'list',
                'workspace_id': 'str',
                'domain_id': 'str',
            }

        Returns:
            budget_vos (object)
            total_count
        """

        query: dict = self._set_user_project_or_project_group_filter(
            params.dict(exclude_unset=True)
        )
        budget_vos, total_count = self.budget_mgr.list_budgets(query)
        budgets_info = [budget_vo.to_dict() for budget_vo in budget_vos]
        return BudgetsResponse(total_count=total_count, results=budgets_info)

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(["domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["budget_id", "name"])
    @set_query_page_limit(1000)
    @convert_model
    def stat(self, params: BudgetStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta,
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.query or {}

        return self.budget_mgr.stat_budgets(query)

    @transaction(exclude=["authentication", "authorization", "mutation"])
    @check_required(["domain_id"])
    def update_budget_utilization_rate(self, params: dict) -> None:
        """
        Args:
            params (dict): {
                'domain_id': 'str',
            }
        Returns:
            None
        """
        domain_id = params["domain_id"]
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")

        query_filter = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "time_unit", "v": "MONTHLY", "o": "eq"},
                {"k": "end", "v": current_month, "o": "gte"},
            ]
        }
        _LOGGER.debug(f"[update_budget_utilization_rate] query_filter: {query_filter}")
        budget_vos, _ = self.budget_mgr.list_budgets(query_filter)

        for budget_vo in budget_vos:
            planned_limits = budget_vo.planned_limits or []

            budget_limit = self._get_budget_limit_from_planned_limits(planned_limits)
            notification = self._reset_plans_from_notification(budget_vo.notification)

            update_params = {
                "utilization_rate": 0,
                "limit": budget_limit,
                "notification": notification,
            }

            budget_vo = self.budget_mgr.update_budget_by_vo(update_params, budget_vo)

            _LOGGER.debug(
                f"[update_budget_utilization_rate] budget_vo: {budget_vo.budget_id}, {budget_vo.utilization_rate})"
            )

    def create_budget_update_job(self, domain_id: str) -> None:
        self.budget_mgr.push_budget_job_task({"domain_id": domain_id})

    @staticmethod
    def _check_time_period(start: str, end: str, budget_year: str = None) -> None:
        if start >= end:
            raise ERROR_INVALID_TIME_RANGE(start=start, end=end)

        if budget_year:
            start_year = start[:4]
            end_year = end[:4]
            if start_year != budget_year or end_year != budget_year:
                raise ERROR_INVALID_PARAMETER(
                    key="budget_year",
                    reason=f"budget_year({budget_year}) should be same as start({start}) or end({end}) year",
                )

    def _check_planned_limits(
        self, start: str, end: str, time_unit: str, planned_limits: list
    ):
        if time_unit == "TOTAL":
            raise ERROR_INVALID_PARAMETER(
                key="time_unit", value=f"Only MONTHLY time_unit is allowed"
            )

        planned_limits_dict = self._convert_planned_limits_data_type(planned_limits)
        date_format = "%Y-%m"

        try:
            start_dt = datetime.strptime(start, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)

        try:
            end_dt = datetime.strptime(end, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="end", type=date_format)

        for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt):
            date_str = dt.strftime(date_format)
            if date_str not in planned_limits_dict:
                raise ERROR_NO_DATE_IN_PLANNED_LIMITS(date=date_str)

            del planned_limits_dict[date_str]

        if len(planned_limits_dict.keys()) > 0:
            raise ERROR_DATE_IS_WRONG(date=list(planned_limits_dict.keys()))

    @staticmethod
    def _convert_planned_limits_data_type(planned_limits: list) -> dict:
        planned_limits_dict = {}

        for planned_limit in planned_limits:
            date = planned_limit.get("date")
            limit = planned_limit.get("limit", 0)
            if date is None:
                raise ERROR_DATE_IS_REQUIRED(value=planned_limit)

            if limit < 0:
                raise ERROR_LIMIT_IS_WRONG(value=planned_limit)

            planned_limits_dict[date] = limit

        return planned_limits_dict

    @staticmethod
    def _check_notification(
        notification: dict,
        domain_id: str,
        workspace_id: str,
    ) -> dict:
        plans = notification.get("plans", [])

        for plan in plans:
            unit = plan["unit"]
            threshold = plan["threshold"]

            if unit not in ["PERCENT"]:
                raise ERROR_UNIT_IS_REQUIRED(value=plan)

            if threshold < 0:
                raise ERROR_THRESHOLD_IS_WRONG(value=plan)

            if unit == "PERCENT":
                if threshold > 100:
                    raise ERROR_THRESHOLD_IS_WRONG_IN_PERCENT_TYPE(value=plan)

        # check recipients
        recipients = notification.get("recipients", {})
        users = list(set(recipients.get("users", [])))
        role_types = list(set(recipients.get("role_types", [])))

        if role_types:
            recipients["role_types"] = role_types

        if users:
            identity_mgr = IdentityManager()
            query = {
                "filter": [
                    {"k": "domain_id", "v": domain_id, "o": "eq"},
                    {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                    {"k": "user_id", "v": users, "o": "in"},
                ]
            }
            rb_response = identity_mgr.list_role_bindings({"query": query}, domain_id)
            rb_infos = rb_response.get("results", [])
            rb_users = [rb_info["user_id"] for rb_info in rb_infos]

            user_response = identity_mgr.list_email_verified_users(domain_id, users)
            user_infos = user_response.get("results", [])
            users = [user_info["user_id"] for user_info in user_infos]

            for user_id in users:
                if user_id not in rb_users:
                    raise ERROR_NOT_FOUND(key="user_id", value=user_id)
            recipients["users"] = users

        notification["recipients"] = recipients
        return notification

    @staticmethod
    def _set_user_project_or_project_group_filter(params: dict) -> dict:
        query = params.get("query", {})
        query["filter"] = query.get("filter", [])

        if "user_projects" in params:
            user_projects = params["user_projects"] + [None]
            query["filter"].append(
                {"k": "user_projects", "v": user_projects, "o": "in"}
            )

        return query

    @staticmethod
    def _check_user_exists(identity_mgr: IdentityManager, user_id: str, domain_id: str):
        response = identity_mgr.list_role_bindings({"user_id": user_id}, domain_id)
        if response.get("total_count", 0) == 0:
            raise ERROR_NOT_FOUND(key="budget_manager_id", value=user_id)

    @staticmethod
    def _get_budget_limit_from_planned_limits(planned_limits: list) -> float:
        budget_limit = 0
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")

        for planned_limit in planned_limits:
            if planned_limit["date"] == current_month:
                budget_limit = planned_limit["limit"]
                break
        return budget_limit

    @staticmethod
    def _reset_plans_from_notification(notification: dict) -> dict:
        plans = notification.get("plans", [])

        for plan in plans:
            plan["notified"] = False

        notification["plans"] = plans
        return notification
