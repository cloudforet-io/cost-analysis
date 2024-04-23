import logging
from typing import Union
from spaceone.core.service import BaseService, transaction
from spaceone.core.service.utils import convert_model
from spaceone.cost_analysis.plugin.data_source.model import (
    JobGetTaskRequest,
    TasksResponse,
)

_LOGGER = logging.getLogger(__name__)


class JobService(BaseService):
    resource = "Job"

    @transaction
    @convert_model
    def get_tasks(self, params: JobGetTaskRequest) -> Union[TasksResponse, dict]:
        """Get job tasks

        Args:
            params (JobGetTaskRequest): {
                'options': 'dict',                  # Required
                'secret_data': 'dict',              # Required
                'linked_accounts': 'list',          # optional
                'schema': 'str',
                'start': 'str',
                'last_synchronized_at': 'datetime',
                'domain_id': 'str'                  # Required
            }

        Returns:
            TasksResponse: {
                'tasks': 'list',
                'changed': 'list'
                'synced_accounts': 'list'
            }

        """

        func = self.get_plugin_method("get_tasks")
        response = func(params.dict())
        return TasksResponse(**response)
