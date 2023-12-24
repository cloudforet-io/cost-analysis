import logging

from spaceone.core.service import *
from spaceone.cost_analysis.manager.job_task_manager import JobTaskManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobTaskService(BaseService):
    resource = "JobTask"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_task_mgr: JobTaskManager = self.locator.get_manager("JobTaskManager")

    @transaction(
        permission="cost-analysis:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["job_task_id", "domain_id"])
    def get(self, params):
        """Get job_task

        Args:
            params (dict): {
                'job_task_id': 'str',
                'domain_id': 'str',
            }

        Returns:
            job_task_vo (object)
        """

        job_task_id = params["job_task_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

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
    def list(self, params):
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
            job_task_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.job_task_mgr.list_job_tasks(query)

    @transaction(
        permission="cost-analysis:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["job_task_id"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.job_task_mgr.stat_job_tasks(query)
