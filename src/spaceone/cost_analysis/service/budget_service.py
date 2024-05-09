import logging
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY, YEARLY

from spaceone.core.service import *
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.budget_model import Budget

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BudgetService(BaseService):
    resource = "Budget"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_mgr: BudgetManager = self.locator.get_manager("BudgetManager")

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(
        [
            "data_source_id",
            "time_unit",
            "start",
            "end",
            "resource_group",
            "domain_id",
        ]
    )
    def create(self, params):
        """Register budget

        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'name': 'str',
                'limit': 'float',
                'planned_limits': 'list',
                'time_unit': 'str',         # required
                'start': 'str',             # required
                'end': 'str',               # required
                'provider_filter': 'dict',
                'notifications': 'list',
                'tags': 'dict',
                'resource_group': 'str',    # required
                'project_id': 'str',
                'workspace_id': 'str',
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            budget_vo (object)
        """

        domain_id = params["domain_id"]
        workspace_id = params["workspace_id"]
        data_source_id = params["data_source_id"]
        project_id = params.get("project_id")
        limit = params.get("limit")
        planned_limits = params.get("planned_limits", [])
        time_unit = params["time_unit"]
        start = params["start"]
        end = params["end"]
        provider_filter = params.get("provider_filter", {})
        provider_filter_state = provider_filter.get("state", "DISABLED")
        notifications = params.get("notifications", [])
        resource_group = params["resource_group"]
        self._check_time_period(start, end)

        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        if resource_group == "WORKSPACE":
            identity_mgr.check_workspace(workspace_id, domain_id)
            params["project_id"] = "*"
            project_id = "*"
        else:
            identity_mgr.get_project(project_id, domain_id)

        # Check Provider Filter
        if provider_filter_state == "ENABLED":
            if len(provider_filter.get("providers", [])) == 0:
                raise ERROR_PROVIDER_FILTER_IS_EMPTY()
        else:
            params["provider_filter"] = {"state": "DISABLED", "providers": []}

        data_source_mgr = DataSourceManager()

        data_source_vos = data_source_mgr.filter_data_sources(
            data_source_id=data_source_id,
            workspace_id=[workspace_id, "*"],
            domain_id=domain_id,
        )

        data_source_vo = data_source_vos.first()

        data_source_metadata = data_source_vo.plugin_info.metadata
        params["currency"] = data_source_metadata.get("currency", "USD")

        if time_unit == "TOTAL":
            if limit is None:
                raise ERROR_REQUIRED_PARAMETER(key="limit")

            params["planned_limits"] = None

        else:
            # Check Planned Limits
            self._check_planned_limits(start, end, time_unit, planned_limits)

            params["limit"] = 0
            for planned_limit in planned_limits:
                params["limit"] += planned_limit.get("limit", 0)

        # Check Notifications
        self._check_notifications(notifications, project_id)

        # Check Duplicated Budget
        budget_vos = self.budget_mgr.filter_budgets(
            data_source_id=data_source_id,
            project_id=project_id,
            workspace_id=workspace_id,
            domain_id=domain_id,
        )
        if budget_vos.count() > 0:
            raise ERROR_BUDGET_ALREADY_EXIST(
                data_source_id=data_source_id,
                workspace_id=workspace_id,
                target=project_id,
            )

        budget_vo = self.budget_mgr.create_budget(params)

        # Create budget usages
        budget_usage_mgr: BudgetUsageManager = self.locator.get_manager(
            "BudgetUsageManager"
        )
        budget_usage_mgr.create_budget_usages(budget_vo)
        budget_usage_mgr.update_cost_usage(budget_vo)
        budget_usage_mgr.notify_budget_usage(budget_vo)

        return budget_vo

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["budget_id", "domain_id"])
    # @change_date_value(['end'])
    def update(self, params):
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

        budget_id = params["budget_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]
        planned_limits = params.get("planned_limits")

        budget_usage_mgr: BudgetUsageManager = self.locator.get_manager(
            "BudgetUsageManager"
        )

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id
        )

        # Check limit and Planned Limits
        budget_vo = self.budget_mgr.update_budget_by_vo(params, budget_vo)

        if "name" in params:
            budget_usage_vos = budget_usage_mgr.filter_budget_usages(
                budget_id=budget_id,
                domain_id=domain_id,
            )
            for budget_usage_vo in budget_usage_vos:
                budget_usage_mgr.update_budget_usage_by_vo(
                    {"name": params["name"]}, budget_usage_vo
                )
        # Check DataSource exists
        data_source_mgr = self.locator.get_manager("DataSourceManager")
        data_source_mgr.get_data_source(budget_vo.data_source_id, domain_id)

        budget_usage_mgr.update_cost_usage(budget_vo)
        budget_usage_mgr.notify_budget_usage(budget_vo)

        return budget_vo

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["budget_id", "workspace_id", "domain_id"])
    def set_notification(self, params):
        """Set budget notification

        Args:
            params (dict): {
                'budget_id': 'str',
                'notifications': 'list',
                'workspace_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            budget_vo (object)
        """
        budget_id = params["budget_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]
        notifications = params.get("notifications", [])

        budget_vo: Budget = self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id
        )

        # Check Notifications
        self._check_notifications(notifications, budget_vo.project_id)
        params["notifications"] = notifications

        return self.budget_mgr.update_budget_by_vo(params, budget_vo)

    @transaction(
        permission="cost-analysis:Budget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["budget_id", "domain_id"])
    def delete(self, params):
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
            params["budget_id"], params["domain_id"], params.get("workspace_id")
        )
        self.budget_mgr.delete_budget_by_vo(budget_vo)

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["budget_id", "domain_id"])
    def get(self, params):
        """Get budget

        Args:
            params (dict): {
                'budget_id': 'str',         # required
                'user_projects': 'list',    # injected from auth (optional)
                'workspace_id': 'str',      # injected from auth (optional)
                'domain_id': 'str',         # injected from auth
            }

        Returns:
            budget_vo (object)
        """

        budget_id = params["budget_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        project_id = params.get("user_projects")

        return self.budget_mgr.get_budget(
            budget_id, domain_id, workspace_id, project_id
        )

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "budget_id",
            "name",
            "time_unit",
            "data_source_id",
            "project_id",
            "user_projects",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["budget_id", "name"])
    def list(self, params):
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

        query: dict = self._set_user_project_or_project_group_filter(params)
        return self.budget_mgr.list_budgets(query)

    @transaction(
        permission="cost-analysis:Budget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["user_projects", "workspace_id", "domain_id"])
    @append_keyword_filter(["budget_id", "name"])
    def stat(self, params):
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

        query = params.get("query", {})
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
    def _check_notifications(notifications, project_id):
        if len(notifications) > 0 and project_id is None:
            raise ERROR_NOTIFICATION_IS_NOT_SUPPORTED_IN_PROJECT(target=project_id)

        for notification in notifications:
            unit = notification.get("unit")
            notification_type = notification.get("notification_type")
            threshold = notification.get("threshold", 0)

            if unit not in ["PERCENT", "ACTUAL_COST"]:
                raise ERROR_UNIT_IS_REQUIRED(value=notification)

            if notification_type not in ["CRITICAL", "WARNING"]:
                raise ERROR_NOTIFICATION_TYPE_IS_REQUIRED(value=notification)

            if threshold < 0:
                raise ERROR_THRESHOLD_IS_WRONG(value=notification)

            if unit == "PERCENT":
                if threshold > 100:
                    raise ERROR_THRESHOLD_IS_WRONG_IN_PERCENT_TYPE(value=notification)

    @staticmethod
    def _set_user_project_or_project_group_filter(params):
        query = params.get("query", {})
        query["filter"] = query.get("filter", [])

        if "user_projects" in params:
            user_projects = params["user_projects"] + [None]
            query["filter"].append(
                {"k": "user_projects", "v": user_projects, "o": "in"}
            )

        return query
