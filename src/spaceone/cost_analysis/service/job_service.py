import copy
import logging
import time
from typing import List, Union
from datetime import timedelta, datetime

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.core import cache, config, utils
from spaceone.cost_analysis.manager.job_manager import JobManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_mgr: JobManager = self.locator.get_manager('JobManager')
