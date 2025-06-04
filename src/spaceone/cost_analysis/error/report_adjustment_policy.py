from spaceone.core.error import *


class ERROR_PROJECT_OR_WORKSPACE_REQUIRED(ERROR_INVALID_ARGUMENT):
    _message = "Project ID and Workspace ID are required when the scope is PROJECT. (scope = {scope}, project_id = {project_id}, workspace_id = {workspace_id})"


class ERROR_SERVICE_ACCOUNT_OR_WORKSPACE_REQUIRED(ERROR_INVALID_ARGUMENT):
    _message = "Service Account ID and Workspace ID are required when the scope is SERVICE_ACCOUNT. (scope = {scope}, service_account_ids = {service_account_ids}, workspace_id = {workspace_id})"


class ERROR_POLICY_ORDER_BELOW_MINIMUM(ERROR_INVALID_ARGUMENT):
    _message = "Order must be greater than or equal to 1. (order = {order})"


class ERROR_POLICY_ORDER_EXCEEDS_MAXIMUM(ERROR_INVALID_ARGUMENT):
    _message = "The provided order exceeds the maximum allowed order for existing policies. (order = {order}, maximum_order = {maximum_order})"
