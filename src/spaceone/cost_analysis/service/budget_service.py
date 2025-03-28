import logging
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from spaceone.core.service import *
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.config_manager import ConfigManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
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
                'time_unit': 'str',         # required
                'start': 'str',             # required
                'end': 'str',               # required
                'notifications': 'dict',
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
        limit = params.limit
        planned_limits = params.planned_limits or []
        time_unit = params.time_unit
        start = params.start
        end = params.end

        notifications = params.notifications or {}
        resource_group = params.resource_group

        self._check_time_period(start, end)

        if resource_group != "PROJECT":
            raise ERROR_NOT_IMPLEMENTED()

        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        identity_mgr.check_workspace(workspace_id, domain_id)
        identity_mgr.get_project(project_id, domain_id)

        if service_account_id:
            identity_mgr.get_service_account(
                service_account_id, domain_id, workspace_id
            )

        if not params.currency:
            config_mgr = ConfigManager()
            unified_cost_config: dict = config_mgr.get_unified_cost_config(domain_id)
            params.currency = unified_cost_config.get("currency", "USD")

        if time_unit == "TOTAL":
            if limit is None:
                raise ERROR_REQUIRED_PARAMETER(key="limit")

            params.planned_limits = None

        else:
            # Check Planned Limits
            self._check_planned_limits(start, end, time_unit, planned_limits)

            params.limit = 0
            for planned_limit in planned_limits:
                params.limit += planned_limit.get("limit", 0)

        # Check Notifications
        self._check_notifications(notifications, domain_id, workspace_id)

        # Check Duplicated Budget
        budget_vos = self.budget_mgr.filter_budgets(
            service_account_id=service_account_id,
            project_id=project_id,
            workspace_id=workspace_id,
            domain_id=domain_id,
        )
        if budget_vos.count() > 0:
            raise ERROR_BUDGET_ALREADY_EXIST(
                service_account_id=service_account_id,
                workspace_id=workspace_id,
                target=service_account_id,
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
                'tags': 'dict'
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

        budget_usage_mgr = BudgetUsageManager()

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id
        )

        # Check limit and Planned Limits
        budget_vo = self.budget_mgr.update_budget_by_vo(
            params.dict(exclude_unset=True), budget_vo
        )

        if "name" in params:
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
                'notifications': 'dict',
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
        notifications = params.notifications or {}

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id, project_id
        )

        # Check Notifications
        self._check_notifications(notifications, domain_id, workspace_id)
        params.notifications = notifications

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
            "budget_id",
            "name",
            "time_unit",
            "project_id",
            "user_projects",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["budget_id", "name"])
    @convert_model
    def list(self, params: BudgetSearchQueryRequest) -> Union[BudgetsResponse, dict]:
        """List budgets

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'budget_id': 'str',
                'name': 'str',
                'time_unit': 'str',
                'data_source_id': 'str',
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
    @check_required(["query", "domain_id"])
    @append_query_filter(["user_projects", "workspace_id", "domain_id"])
    @append_keyword_filter(["budget_id", "name"])
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

    @staticmethod
    def _check_time_period(start, end):
        if start >= end:
            raise ERROR_INVALID_TIME_RANGE(start=start, end=end)

    def _check_planned_limits(self, start, end, time_unit, planned_limits):
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
    def _convert_planned_limits_data_type(planned_limits):
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
    def _check_notifications(
        notifications: dict,
        domain_id: str,
        workspace_id: str,
    ) -> dict:
        plans = notifications.get("plans", [])

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
        recipients = notifications.get("recipients", {})
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

        notifications["recipients"] = recipients
        return notifications

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
