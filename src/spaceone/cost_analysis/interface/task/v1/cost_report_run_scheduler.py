import logging
from datetime import datetime, timezone

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler

_LOGGER = logging.getLogger(__name__)


class CostReportRunScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=":00"):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()

    def _init_config(self):
        self._token = config.get_global("TOKEN")
        if self._token is None:
            raise ERROR_CONFIGURATION(key="TOKEN")
        self._cost_report_sync_hour = config.get_global("COST_REPORT_RUN_HOUR", 0)
        _LOGGER.debug(
            f"[CostReportRunScheduler] _cost_report_sync_hour: {self._cost_report_sync_hour}"
        )

    def create_task(self) -> list:
        tasks = []
        tasks.extend(self._create_cost_report_run_task())
        return tasks

    def _create_cost_report_run_task(self):
        if datetime.now(timezone.utc).hour == self._cost_report_sync_hour:
            stp = {
                "name": "cost_report_data_sync_schedule",
                "version": "v1",
                "executionEngine": "BaseWorker",
                "stages": [
                    {
                        "locator": "SERVICE",
                        "name": "CostReportService",
                        "metadata": {"token": self._token},
                        "method": "create_cost_report_by_cost_report_config",
                        "params": {"params": {}},
                    }
                ],
            }
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] create_cost_report_by_cost_report_config => START"
            )
            return [stp]
        else:
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] create_cost_report_by_cost_report_config => SKIP"
            )
            print(
                f"{utils.datetime_to_iso8601(datetime.now(timezone.utc))} [INFO] [create_task] cost_report_sync_time: {self._cost_report_sync_hour} hour (UTC)"
            )
            return []
