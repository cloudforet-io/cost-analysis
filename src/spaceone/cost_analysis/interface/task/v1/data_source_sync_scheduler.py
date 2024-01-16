import logging
from datetime import datetime

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler

_LOGGER = logging.getLogger(__name__)


class DataSourceSyncScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=":00"):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()

    def _init_config(self):
        self._token = config.get_global("TOKEN")
        if self._token is None:
            raise ERROR_CONFIGURATION(key="TOKEN")
        self._data_source_sync_hour = config.get_global("DATA_SOURCE_SYNC_HOUR", 16)

    def create_task(self):
        if datetime.utcnow().hour == self._data_source_sync_hour:
            stp = {
                "name": "data_source_sync_schedule",
                "version": "v1",
                "executionEngine": "BaseWorker",
                "stages": [
                    {
                        "locator": "SERVICE",
                        "name": "JobService",
                        "metadata": {"token": self._token},
                        "method": "create_jobs_by_data_source",
                        "params": {"params": {}},
                    }
                ],
            }

            print(
                f"{utils.datetime_to_iso8601(datetime.now())} [INFO] [create_task] create_jobs_by_data_source => START"
            )
            return [stp]
        else:
            print(
                f"{utils.datetime_to_iso8601(datetime.now())} [INFO] [create_task] create_jobs_by_data_source => SKIP"
            )
            print(
                f"{utils.datetime_to_iso8601(datetime.now())} [INFO] [create_task] data_source_sync_time: {self._data_source_sync_hour} hour (UTC)"
            )
            return []
