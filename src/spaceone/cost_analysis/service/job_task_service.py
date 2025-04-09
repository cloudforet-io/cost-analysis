import logging
from typing import Union

from spaceone.core.service import *
from spaceone.cost_analysis.manager.job_task_manager import JobTaskManager
from spaceone.cost_analysis.model.job_task.request import JobTaskGetRequest, JobTaskSearchQueryRequest, \
    JobTaskStatQueryRequest
from spaceone.cost_analysis.model.job_task.response import JobTaskResponse, JobTasksResponse

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobTaskService(BaseService):
    resource = "JobTask"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_task_mgr = JobTaskManager()

    @transaction(
        permission="cost-analysis:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["job_task_id", "domain_id"])
    def get(self, params: JobTaskGetRequest) -> Union[JobTaskResponse, dict]:
        """Get job_task

        Args:
            params (dict): {
                'job_task_id': 'str',
                'domain_id': 'str',
            }

        Returns:
            job_task_vo (object)
        """

        job_task_id = params.job_task_id
        workspace_id = params.workspace_id
        domain_id = params.domain_id

        return self.job_task_mgr.get_job_task(job_task_id, domain_id, workspace_id)

    @transaction(
        permission="cost-analysis:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["domain_id"])
    @append_query_filter(
        ["job_task_id", "status", "job_id", "data_source_id", "domain_id"]
    )
    @append_keyword_filter(["job_task_id"])
    def list(self, params: JobTaskSearchQueryRequest) -> Union[JobTasksResponse, dict]:
        """List job_tasks

        Args:
            params (dict): {
                'job_task_id': 'str',
                'status': 'str',
                'job_id': 'str',
                'data_source_id': 'str',
                'workspace_id': 'list',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            JobTaskResponses
            total_count
        """

        query = params.query or {}

        job_task_data_vos, total_count = self.job_task_mgr.list_job_tasks(query)
        job_tasks_data_info = [job_task_data_vo.to_dict() for job_task_data_vo in job_task_data_vos]
        return JobTasksResponse(results=job_tasks_data_info, total_count=total_count)

    @transaction(
        permission="cost-analysis:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["job_task_id"])
    def stat(self, params: JobTaskStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.query or {}
        return self.job_task_mgr.stat_job_tasks(query)
