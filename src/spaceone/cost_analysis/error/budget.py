from spaceone.core.error import *


class ERROR_ONLY_ONF_OF_PROJECT_OR_PROJECT_GROUP(ERROR_INVALID_ARGUMENT):
    _message = "Only one of project_id or project_group_id is allowed."


class ERROR_INVALID_TIME_RANGE(ERROR_INVALID_ARGUMENT):
    _message = "Budget end time must be greater than start time. (start = {start}, end = {end})"


class ERROR_NO_DATE_IN_PLANNED_LIMITS(ERROR_INVALID_ARGUMENT):
    _message = "No date in the planned limits. (date = {date})"


class ERROR_DATE_IS_REQUIRED(ERROR_INVALID_ARGUMENT):
    _message = (
        "Date is required for planned limits. (key = planned_limits, value = {value})"
    )


class ERROR_LIMIT_IS_WRONG(ERROR_INVALID_ARGUMENT):
    _message = (
        "Limit must be greater than zero. (key = planned_limits, value = {value})"
    )


class ERROR_DATE_IS_WRONG(ERROR_INVALID_ARGUMENT):
    _message = "Date is wrong in the planned limits. (wrong date = {date})"


class ERROR_UNIT_IS_REQUIRED(ERROR_INVALID_ARGUMENT):
    _message = "Unit is required for notification (key = notification, value = {value})"


class ERROR_NOTIFICATION_TYPE_IS_REQUIRED(ERROR_INVALID_ARGUMENT):
    _message = "Notification type is required for notification (key = notification, value = {value})"


class ERROR_THRESHOLD_IS_WRONG(ERROR_INVALID_ARGUMENT):
    _message = (
        "Threshold must be greater than zero. (key = notification, value = {value})"
    )


class ERROR_THRESHOLD_IS_WRONG_IN_PERCENT_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "In percentage type, the threshold must be less than 100. (key = notification, value = {value})"


class ERROR_PROVIDER_FILTER_IS_EMPTY(ERROR_INVALID_ARGUMENT):
    _message = "Provider filter is empty. (key = provider_filter.providers, value = [])"


class ERROR_BUDGET_ALREADY_EXIST(ERROR_INVALID_ARGUMENT):
    _message = "Budget already exist. (start = {start} end = {end}, target = {target}, workspace_id = {workspace_id})"


class ERROR_NOTIFICATION_IS_NOT_SUPPORTED_IN_PROJECT(ERROR_INVALID_ARGUMENT):
    _message = "Notification is not supported in project. (target = {target})"


class ERROR_BUDGET_MANAGER_IS_NOT_VERIFIED(ERROR_INVALID_ARGUMENT):
    _message = "To assign as a budget manager to a user, email verification is required. (user_id = {user_id})"


class ERROR_DUPLICATED_THRESHOLD(ERROR_INVALID_ARGUMENT):
    _message = "Duplicate threshold in notification. (threshold = {threshold})"
