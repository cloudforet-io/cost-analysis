from spaceone.core.error import *


class ERROR_COST_REPORT_CONFIG_NOT_ENABLED(ERROR_INVALID_ARGUMENT):
    _message = "Cost report config is not enabled. (cost_report_config_id = {cost_report_config_id}, state = {state})"
