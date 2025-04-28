import logging
from datetime import datetime, timezone

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler

_LOGGER = logging.getLogger(__name__)


class BudgetUpdateScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=":00"):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()

    def _init_config(self):
        self._token = config.get_global("TOKEN")
        if self._token is None:
            raise ERROR_CONFIGURATION(key="TOKEN")
        self._budget_update_hour = config.get_global("BUDGET_UPDATE_HOUR", 0)
        self._budget_update_day = config.get_global("BUDGET_UPDATE_DAY", 1)

        if self._budget_update_hour < 0 or self._budget_update_hour > 23:
            _LOGGER.warning(
                f"Invalid BUDGET_UPDATE_HOUR: {self._budget_update_hour} hour (UTC). Must be between 0 and 23."
            )
            self._budget_update_hour = 0

        if self._budget_update_day < 1 or self._budget_update_day > 31:
            _LOGGER.warning(
                f"Invalid BUDGET_UPDATE_DAY: {self._budget_update_day} day. Must be between 1 and 31."
            )
            self._budget_update_day = 1

        _LOGGER.debug(
            f"[BudgetUpdateScheduler] _budget_update_hour: {self._budget_update_hour}, _budget_update_day: {self._budget_update_day}"
        )

    def create_task(self) -> list:
        tasks = []
        tasks.extend(self._create_budget_utilization_rate_update_task())
        tasks.extend(self._create_budget_state_update_task())
        return tasks

    def _create_budget_utilization_rate_update_task(self):

        current_day = datetime.now(timezone.utc).day
        if (
            current_day == self._budget_update_day
            and datetime.now(timezone.utc).hour == self._budget_update_hour
        ):

            stp = {
                "name": "budget_update_schedule",
                "version": "v1",
                "executionEngine": "BaseWorker",
                "stages": [
                    {
                        "locator": "SERVICE",
                        "name": "BudgetService",
                        "metadata": {"token": self._token},
                        "method": "create_budget_update_job_by_domain",
                        "params": {"params": {}},
                    }
                ],
            }

            print(
                f"{utils.datetime_to_iso8601(datetime.utcnow())} [INFO] [create_task] create_budget_update_job_by_domain => START"
            )
            return [stp]
        else:
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] create_budget_update_job_by_domain => SKIP"
            )
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] budget_utilization_update_hour: {self._budget_update_hour} hour (UTC)"
            )
            return []

    def _create_budget_state_update_task(self):
        if datetime.now(timezone.utc).hour == self._budget_update_hour:
            stp = {
                "name": "budget_state_update_schedule",
                "version": "v1",
                "executionEngine": "BaseWorker",
                "stages": [
                    {
                        "locator": "SERVICE",
                        "name": "BudgetService",
                        "metadata": {"token": self._token},
                        "method": "update_budget_state_job_by_domain",
                        "params": {"params": {}},
                    }
                ],
            }

            print(
                f"{utils.datetime_to_iso8601(datetime.utcnow())} [INFO] [create_task] update_budget_state_by_domain => START"
            )
            return [stp]
        else:
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] update_budget_state_by_domain => SKIP"
            )
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] budget_state_update_hour: {self._budget_update_hour} hour (UTC)"
            )
            return []
