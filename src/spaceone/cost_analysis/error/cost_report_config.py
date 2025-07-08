from spaceone.core.error import *


class ERROR_CANT_REGENERATE_REPORT_FOR_THIS_MONTH(ERROR_INVALID_ARGUMENT):
    _message = "You cannot regenerate the report for this month. (report_month = {report_month}) Please use the 'run' command to generate this month's report."

class ERROR_COST_REPORT_CONFIG_NOT_ENABLED(ERROR_INVALID_ARGUMENT):
    _message = "Cost report config is not enabled. (cost_report_config_id = {cost_report_config_id}, state = {state})"
