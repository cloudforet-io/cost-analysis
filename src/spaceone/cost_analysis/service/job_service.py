import copy
import datetime
import logging
from datetime import timedelta, datetime
from typing import Dict

from dateutil.relativedelta import relativedelta

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model import DataSourceAccount
from spaceone.cost_analysis.model.job_task_model import JobTask
from spaceone.cost_analysis.model.job_model import Job
from spaceone.cost_analysis.model.data_source_model import DataSource
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.data_source_account_manager import (
    DataSourceAccountManager,
)
from spaceone.cost_analysis.manager.job_manager import JobManager
from spaceone.cost_analysis.manager.job_task_manager import JobTaskManager
from spaceone.cost_analysis.manager.data_source_plugin_manager import (
    DataSourcePluginManager,
)
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobService(BaseService):
    resource = "Job"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager("CostManager")
        self.job_mgr: JobManager = self.locator.get_manager("JobManager")
        self.job_task_mgr: JobTaskManager = self.locator.get_manager("JobTaskManager")
        self.data_source_mgr: DataSourceManager = self.locator.get_manager(
            "DataSourceManager"
        )
        self.ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager(
            "DataSourcePluginManager"
        )
        self.budget_usage_mgr: BudgetUsageManager = self.locator.get_manager(
            "BudgetUsageManager"
        )
        self.data_source_account_mgr = DataSourceAccountManager()

    @transaction(exclude=["authentication", "authorization", "mutation"])
    def create_jobs_by_data_source(self, params):
        """Create jobs by domain

        Args:
            params (dict): {}

        Returns:
            None
        """

        for data_source_vo in self._get_all_data_sources():
            try:
                self.create_cost_job(data_source_vo, {"sync_mode": "SCHEDULED"})
            except Exception as e:
                _LOGGER.error(
                    f"[create_jobs_by_data_source] sync error: {e}", exc_info=True
                )

    @transaction(
        permission="cost-analysis:Job.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["job_id", "domain_id"])
    def cancel(self, params):
        """Get job

        Args:
            params (dict): {
                'job_id': 'str',        # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str'      # injected from auth
            }

        Returns:
            job_vo (object)
        """

        job_id = params["job_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        job_vo = self.job_mgr.get_job(job_id, domain_id, workspace_id)

        if job_vo.status != "IN_PROGRESS":
            raise ERROR_JOB_STATE(job_state=job_vo.status)

        return self.job_mgr.change_canceled_status(job_vo)

    @transaction(
        permission="cost-analysis:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["job_id", "domain_id"])
    def get(self, params):
        """Get job

        Args:
            params (dict): {
                'job_id': 'str',        # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth
            }

        Returns:
            job_vo (object)
        """

        job_id = params["job_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        return self.job_mgr.get_job(job_id, domain_id, workspace_id)

    @transaction(
        permission="cost-analysis:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["domain_id"])
    @append_query_filter(
        ["job_id", "status", "data_source_id", "workspace_id", "domain_id"]
    )
    @append_keyword_filter(["job_id"])
    def list(self, params):
        """List jobs

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'status': 'str',
                'data_source_id': 'str',
                'job_id': 'str',
                'workspace_id': 'list',
                'domain_id': 'str',                             # injected from auth
            }

        Returns:
            job_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.job_mgr.list_jobs(query)

    @transaction(
        permission="cost-analysis:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["job_id"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'workspace_id': 'list,
                'domain_id': 'str',
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.job_mgr.stat_jobs(query)

    @transaction(exclude=["authentication", "authorization", "mutation"])
    @check_required(["task_options", "job_task_id", "domain_id"])
    def get_cost_data(self, params):
        """Execute task to get cost data

        Args:
            params (dict): {
                'task_options': 'dict',
                'job_task_id': 'str',
                'secret_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        task_options = params["task_options"]
        job_task_id = params["job_task_id"]
        secret_id = params["secret_id"]
        domain_id = params["domain_id"]
        cost_data_options = {}

        job_task_vo: JobTask = self.job_task_mgr.get_job_task(job_task_id, domain_id)
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(
            job_task_vo.data_source_id, domain_id, job_task_vo.workspace_id
        )
        plugin_info = data_source_vo.plugin_info.to_dict()
        secret_type = data_source_vo.secret_type

        data_source_id = data_source_vo.data_source_id
        job_id = job_task_vo.job_id

        if self._is_job_failed(job_id, domain_id, job_task_vo.workspace_id):
            self.job_task_mgr.change_canceled_status(job_task_vo)
        else:
            job_task_vo = self.job_task_mgr.change_in_progress_status(job_task_vo)

            try:
                options = plugin_info.get("options", {})
                schema_id = plugin_info.get("schema_id")
                schema = None
                tag_keys = data_source_vo.cost_tag_keys
                additional_info_keys = data_source_vo.cost_additional_info_keys
                data_keys = data_source_vo.cost_data_keys
                secret_type = data_source_vo.secret_type
                options.update({"secret_type": secret_type})

                secret_data = self._get_secret_data(secret_id, domain_id)

                if secret_type == "USE_SERVICE_ACCOUNT_SECRET":
                    (
                        service_account_id,
                        project_id,
                    ) = self._get_service_account_id_and_project_id(
                        params.get("secret_id"), domain_id
                    )
                    cost_data_options.update(
                        {
                            "service_account_id": service_account_id,
                            "project_id": project_id,
                        }
                    )

                (
                    endpoint,
                    updated_version,
                ) = self.ds_plugin_mgr.get_data_source_plugin_endpoint(
                    plugin_info, domain_id
                )

                self.ds_plugin_mgr.initialize(endpoint)
                start_dt = datetime.utcnow()

                count = 0
                is_canceled = False

                for costs_data in self.ds_plugin_mgr.get_cost_data(
                    options, secret_data, schema, task_options, domain_id
                ):
                    results = costs_data.get("results", [])
                    for cost_data in results:
                        count += 1

                        self._check_cost_data(cost_data)
                        self._create_cost_data(
                            cost_data, job_task_vo, cost_data_options
                        )

                        tag_keys = self._append_tag_keys(tag_keys, cost_data)
                        additional_info_keys = self._append_additional_info_keys(
                            additional_info_keys, cost_data
                        )
                        data_keys = self._append_data_keys(data_keys, cost_data)

                    if self._is_job_failed(job_id, domain_id, job_task_vo.workspace_id):
                        self.job_task_mgr.change_canceled_status(job_task_vo)
                        is_canceled = True
                        break
                    else:
                        job_task_vo = self.job_task_mgr.update_sync_status(
                            job_task_vo, len(results)
                        )

                if not is_canceled:
                    end_dt = datetime.utcnow()
                    _LOGGER.debug(f"[get_cost_data] end job ({job_task_id}): {end_dt}")
                    _LOGGER.debug(
                        f"[get_cost_data] total job time ({job_task_id}): {end_dt - start_dt}"
                    )

                    self._update_keys(
                        data_source_vo, tag_keys, additional_info_keys, data_keys
                    )
                    self.job_task_mgr.change_success_status(job_task_vo, count)

            except Exception as e:
                self.job_task_mgr.change_error_status(job_task_vo, e, secret_type)

        self._close_job(
            job_id,
            data_source_id,
            domain_id,
            data_source_vo.cost_data_keys,
            data_source_vo.cost_additional_info_keys,
            data_source_vo.cost_tag_keys,
            job_task_vo.workspace_id,
        )

    def create_cost_job(self, data_source_vo: DataSource, job_options):
        tasks = []
        changed = []
        synced_accounts = []

        resource_group = data_source_vo.resource_group
        data_source_id = data_source_vo.data_source_id
        workspace_id = data_source_vo.workspace_id
        domain_id = data_source_vo.domain_id

        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(
            data_source_vo
        )
        options = data_source_vo.plugin_info.options
        schema_id = data_source_vo.plugin_info.schema_id
        schema = None

        if data_source_vo.secret_type:
            secret_type = data_source_vo.secret_type
        else:
            secret_type = "MANUAL"

        options.update({"secret_type": secret_type})
        secret_ids = self._list_secret_ids_from_secret_type(
            data_source_vo, secret_type, workspace_id, domain_id
        )

        self.ds_plugin_mgr.initialize(endpoint)
        params = {
            "last_synchronized_at": data_source_vo.last_synchronized_at,
            "start": job_options.get("start"),
        }

        start, last_synchronized_at = self._get_start_last_synchronized_at(params)

        for secret_id in secret_ids:
            try:
                secret_data = self._get_secret_data(secret_id, domain_id)

                linked_accounts = self._get_linked_accounts_from_data_source_vo(
                    data_source_vo, options, secret_data, schema
                )

                (
                    single_tasks,
                    single_changed,
                    single_synced_accounts,
                ) = self.ds_plugin_mgr.get_tasks(
                    options,
                    secret_id,
                    secret_data,
                    start,
                    last_synchronized_at,
                    domain_id,
                    schema,
                    linked_accounts,
                )
                tasks.extend(single_tasks)
                changed.extend(single_changed)
                if single_synced_accounts:
                    synced_accounts.extend(
                        [
                            {"account_id": single_synced_account["account_id"]}
                            for single_synced_account in single_synced_accounts
                        ]
                    )

            except Exception as e:
                _LOGGER.error(f"[create_cost_job] get_tasks error: {e}", exc_info=True)

                if secret_type == "MANUAL":
                    raise ERROR_GET_JOB_TASKS(
                        secret_id=secret_id, data_source_id=data_source_id, reason=e
                    )

        for task in tasks:
            _LOGGER.debug(f'[sync] task options: {task["task_options"]}')
        _LOGGER.debug(f"[sync] changed: {changed}")

        # Add Job Options
        job_vo = self.job_mgr.create_job(
            resource_group,
            data_source_id,
            workspace_id,
            domain_id,
            job_options,
            len(tasks),
            changed,
            synced_accounts,
        )

        if self._check_duplicate_job(data_source_id, domain_id, job_vo):
            self.job_mgr.change_error_status(
                job_vo, ERROR_DUPLICATE_JOB(data_source_id=data_source_id)
            )
        else:
            if len(tasks) > 0:
                for task in tasks:
                    job_task_vo = None
                    task_options = task["task_options"]
                    try:
                        job_task_vo = self.job_task_mgr.create_job_task(
                            job_vo.resource_group,
                            job_vo.job_id,
                            data_source_id,
                            job_vo.workspace_id,
                            domain_id,
                            task_options,
                        )
                        self.job_task_mgr.push_job_task(
                            {
                                "task_options": task_options,
                                "secret_id": task.get("secret_id"),
                                "secret_data": task.get("secret_data", {}),
                                "job_task_id": job_task_vo.job_task_id,
                                "domain_id": domain_id,
                            }
                        )
                    except Exception as e:
                        if job_task_vo:
                            self.job_task_mgr.change_error_status(
                                job_task_vo, e, secret_type
                            )
            else:
                job_vo = self.job_mgr.change_success_status(job_vo)
                self.data_source_mgr.update_data_source_by_vo(
                    {"last_synchronized_at": job_vo.created_at}, data_source_vo
                )

        return job_vo

    def _list_secret_ids_from_secret_type(
        self,
        data_source_vo: DataSource,
        secret_type: str,
        workspace_id: str,
        domain_id: str,
    ):
        secret_ids = []

        if secret_type == "MANUAL":
            secret_ids = [data_source_vo.plugin_info.secret_id]

        elif secret_type == "USE_SERVICE_ACCOUNT_SECRET":
            secret_filter = {}
            provider = data_source_vo.provider

            if data_source_vo.secret_filter:
                secret_filter = data_source_vo.secret_filter.to_dict()

            secret_ids = self._list_secret_ids_from_secret_filter(
                secret_filter, provider, workspace_id, domain_id
            )

        return secret_ids

    def _list_secret_ids_from_secret_filter(
        self, secret_filter, provider: str, workspace_id: str, domain_id: str
    ):
        secret_manager: SecretManager = self.locator.get_manager(SecretManager)

        _filter = self._set_secret_filter(
            secret_filter, provider, workspace_id, domain_id
        )
        query = {"filter": _filter} if _filter else {}
        response = secret_manager.list_secrets(query, domain_id)
        return [
            secret_info.get("secret_id") for secret_info in response.get("results", [])
        ]

    @staticmethod
    def _set_secret_filter(
        secret_filter, provider: str, workspace_id: str, domain_id: str
    ):
        _filter = [{"k": "domain_id", "v": domain_id, "o": "eq"}]

        if provider:
            _filter.append({"k": "provider", "v": provider, "o": "eq"})
        if workspace_id:
            _filter.append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        if secret_filter and secret_filter.get("state") == "ENABLED":
            if "secrets" in secret_filter and secret_filter["secrets"]:
                _filter.append(
                    {"k": "secret_id", "v": secret_filter["secrets"], "o": "in"}
                )
            if (
                "service_accounts" in secret_filter
                and secret_filter["service_accounts"]
            ):
                _filter.append(
                    {
                        "k": "service_account_id",
                        "v": secret_filter["service_accounts"],
                        "o": "in",
                    }
                )
            if "schemas" in secret_filter and secret_filter["schemas"]:
                _filter.append(
                    {"k": "schema_id", "v": secret_filter["schemas"], "o": "in"}
                )

        return _filter

    def _get_service_account_id_and_project_id(self, secret_id, domain_id):
        service_account_id = None
        project_id = None

        secret_mgr: SecretManager = self.locator.get_manager(SecretManager)

        if secret_id:
            _query = {"filter": [{"k": "secret_id", "v": secret_id, "o": "eq"}]}
            response = secret_mgr.list_secrets(_query, domain_id)
            results = response.get("results", [])
            if results:
                secret_info = results[0]
                service_account_id = secret_info.get("service_account_id")
                project_id = secret_info.get("project_id")

        return service_account_id, project_id

    @staticmethod
    def _append_tag_keys(tags_keys, cost_data):
        cost_tags = cost_data.get("tags") or {}

        for key in cost_tags.keys():
            if key not in tags_keys:
                tags_keys.append(key)
        return tags_keys

    @staticmethod
    def _append_additional_info_keys(additional_info_keys, cost_data):
        cost_additional_info = cost_data.get("additional_info") or {}

        for key in cost_additional_info.keys():
            if key not in additional_info_keys:
                additional_info_keys.append(key)
        return additional_info_keys

    @staticmethod
    def _append_data_keys(data_keys, cost_data) -> list:
        cost_data_info = cost_data.get("data") or {}

        for key in cost_data_info.keys():
            if key not in data_keys:
                data_keys.append(key)
        return data_keys

    def _get_secret_data(self, secret_id: str, domain_id: str) -> dict:
        # todo: this method is internal method
        secret_mgr: SecretManager = self.locator.get_manager("SecretManager")
        if secret_id:
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
        else:
            secret_data = {}

        return secret_data

    @staticmethod
    def _check_cost_data(cost_data):
        if "billed_date" not in cost_data:
            _LOGGER.error(f"[_check_cost_data] cost_data: {cost_data}")
            raise ERROR_REQUIRED_PARAMETER(key="plugin_cost_data.billed_date")

    def _create_cost_data(self, cost_data, job_task_vo, cost_options):
        cost_data["cost"] = cost_data.get("cost", 0)
        cost_data["job_id"] = job_task_vo.job_id
        cost_data["job_task_id"] = job_task_vo.job_task_id
        cost_data["data_source_id"] = job_task_vo.data_source_id
        cost_data["domain_id"] = job_task_vo.domain_id
        cost_data["billed_date"] = cost_data["billed_date"]

        if "service_account_id" in cost_options:
            cost_data["service_account_id"] = cost_options["service_account_id"]

        if "project_id" in cost_options:
            cost_data["project_id"] = cost_options["project_id"]

        if job_task_vo.resource_group == "WORKSPACE":
            cost_data["workspace_id"] = job_task_vo.workspace_id

        self.cost_mgr.create_cost(cost_data, execute_rollback=False)

    def _is_job_failed(
        self,
        job_id: str,
        domain_id: str,
        workspace_id: str,
    ):
        job_vo: Job = self.job_mgr.get_job(job_id, domain_id, workspace_id)

        if job_vo.status in ["CANCELED", "FAILURE"]:
            return True
        else:
            return False

    def _close_job(
        self,
        job_id: str,
        data_source_id: str,
        domain_id: str,
        data_keys: list,
        additional_info_keys: list,
        tag_keys: list,
        workspace_id: str = None,
    ) -> None:
        job_vo: Job = self.job_mgr.get_job(job_id, domain_id, workspace_id)
        no_preload_cache = job_vo.options.get("no_preload_cache", False)

        if job_vo.remained_tasks == 0:
            if job_vo.status == "IN_PROGRESS":
                try:
                    self._aggregate_cost_data(
                        job_vo, data_keys, additional_info_keys, tag_keys
                    )

                    for changed_vo in job_vo.changed:
                        self._delete_changed_cost_data(
                            job_vo,
                            changed_vo.start,
                            changed_vo.end,
                            changed_vo.filter,
                            domain_id,
                        )

                except Exception as e:
                    _LOGGER.error(
                        f"[_close_job] aggregate cost data error: {e}", exc_info=True
                    )
                    self._rollback_cost_data(job_vo)
                    self.job_mgr.change_error_status(
                        job_vo, f"aggregate cost data error: {e}"
                    )
                    raise e

                try:
                    self._delete_old_cost_data(data_source_id, domain_id)
                except Exception as e:
                    _LOGGER.error(
                        f"[_close_job] delete old cost data error: {e}", exc_info=True
                    )
                    self.job_mgr.change_error_status(
                        job_vo, f"delete old cost data error: {e}"
                    )
                    raise e

                try:
                    self.cost_mgr.remove_stat_cache(domain_id, data_source_id)

                    if not no_preload_cache:
                        self.job_mgr.preload_cost_stat_queries(
                            domain_id, data_source_id
                        )

                    self.budget_usage_mgr.update_budget_usage(domain_id, data_source_id)
                    self._update_last_sync_time(job_vo)
                    self._update_data_source_is_synced(job_vo)
                    self.job_mgr.change_success_status(job_vo)

                except Exception as e:
                    _LOGGER.error(
                        f"[_close_job] cache and budget update error: {e}",
                        exc_info=True,
                    )
                    self.job_mgr.change_error_status(
                        job_vo, f"cache and budget update error: {e}"
                    )
                    raise e

            elif job_vo.status == "ERROR":
                self._rollback_cost_data(job_vo)
                self.job_mgr.update_job_by_vo(
                    {"finished_at": datetime.utcnow()}, job_vo
                )

            elif job_vo.status == "CANCELED":
                self._rollback_cost_data(job_vo)

    def _update_keys(self, data_source_vo, tag_keys, additional_info_keys, data_keys):
        self.data_source_mgr.update_data_source_by_vo(
            {
                "cost_tag_keys": tag_keys,
                "cost_additional_info_keys": additional_info_keys,
                "cost_data_keys": data_keys,
            },
            data_source_vo,
        )

    def _rollback_cost_data(self, job_vo: Job):
        cost_vos = self.cost_mgr.filter_costs(
            data_source_id=job_vo.data_source_id,
            domain_id=job_vo.domain_id,
            job_id=job_vo.job_id,
        )

        _LOGGER.debug(
            f"[_close_job] delete cost data created by job: {job_vo.job_id} (count = {cost_vos.count()})"
        )
        cost_vos.delete()

        monthly_cost_vos = self.cost_mgr.filter_monthly_costs(
            data_source_id=job_vo.data_source_id,
            domain_id=job_vo.domain_id,
            job_id=job_vo.job_id,
        )

        _LOGGER.debug(
            f"[_close_job] delete monthly cost data created by job: {job_vo.job_id} (count = {cost_vos.count()})"
        )
        monthly_cost_vos.delete()

    def _update_last_sync_time(self, job_vo: Job):
        self.data_source_mgr: DataSourceManager = self.locator.get_manager(
            "DataSourceManager"
        )
        data_source_vo = self.data_source_mgr.get_data_source(
            job_vo.data_source_id, job_vo.domain_id
        )
        self.data_source_mgr.update_data_source_by_vo(
            {"last_synchronized_at": job_vo.created_at}, data_source_vo
        )

    def _delete_old_cost_data(self, data_source_id: str, domain_id: str):
        now = datetime.utcnow().date()
        old_billed_month = (now - relativedelta(months=12)).strftime("%Y-%m")
        old_billed_year = (now - relativedelta(months=36)).strftime("%Y")

        cost_delete_query = {
            "filter": [
                {"k": "billed_month", "v": old_billed_month, "o": "lt"},
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }

        cost_vos, total_count = self.cost_mgr.list_costs(
            cost_delete_query, domain_id, data_source_id
        )
        _LOGGER.debug(f"[_delete_old_cost_data] delete costs (count = {total_count})")
        cost_vos.delete()

        monthly_cost_delete_query = {
            "filter": [
                {"k": "billed_year", "v": old_billed_year, "o": "lt"},
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }

        monthly_cost_vos, total_count = self.cost_mgr.list_monthly_costs(
            monthly_cost_delete_query, domain_id
        )
        _LOGGER.debug(
            f"[_delete_old_cost_data] delete monthly costs (count = {total_count})"
        )
        monthly_cost_vos.delete()

    def _delete_changed_cost_data(
        self, job_vo: Job, start, end, change_filter, domain_id
    ):
        query = {
            "filter": [
                {"k": "billed_month", "v": start, "o": "gte"},
                {"k": "data_source_id", "v": job_vo.data_source_id, "o": "eq"},
                {"k": "domain_id", "v": job_vo.domain_id, "o": "eq"},
                {"k": "job_id", "v": job_vo.job_id, "o": "not"},
            ]
        }

        if end:
            query["filter"].append({"k": "billed_month", "v": end, "o": "lte"})

        for key, value in change_filter.items():
            query["filter"].append({"k": key, "v": value, "o": "eq"})

        _LOGGER.debug(f"[_delete_changed_cost_data] query: {query}")

        cost_vos, total_count = self.cost_mgr.list_costs(
            copy.deepcopy(query), domain_id, job_vo.data_source_id
        )
        cost_vos.delete()
        _LOGGER.debug(
            f"[_delete_changed_cost_data] delete costs (count = {total_count})"
        )

        monthly_cost_vos, total_count = self.cost_mgr.list_monthly_costs(
            copy.deepcopy(query), domain_id
        )
        monthly_cost_vos.delete()
        _LOGGER.debug(
            f"[_delete_changed_cost_data] delete monthly costs (count = {total_count})"
        )

    def _aggregate_cost_data(
        self, job_vo: Job, data_keys: list, additional_info_keys: list, tag_keys: list
    ):
        data_source_id = job_vo.data_source_id
        domain_id = job_vo.domain_id
        job_id = job_vo.job_id
        job_task_ids = self._get_job_task_ids(job_id, domain_id)

        for job_task_id in job_task_ids:
            for billed_month in self._distinct_billed_month(
                domain_id, data_source_id, job_id, job_task_id
            ):
                self._aggregate_monthly_cost_data(
                    data_source_id,
                    domain_id,
                    job_id,
                    job_task_id,
                    billed_month,
                    data_keys,
                    additional_info_keys,
                    tag_keys,
                )

    def _distinct_billed_month(
        self, domain_id: str, data_source_id: str, job_id: str, job_task_id: str
    ):
        query = {
            "distinct": "billed_month",
            "filter": [
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "job_id", "v": job_id, "o": "eq"},
                {"k": "job_task_id", "v": job_task_id, "o": "eq"},
            ],
            "target": "PRIMARY",  # Execute a query to primary DB
        }
        _LOGGER.debug(f"[_distinct_cost_data] query: {query}")
        response = self.cost_mgr.stat_costs(query, domain_id, data_source_id)
        values = response.get("results", [])

        _LOGGER.debug(f"[_distinct_cost_data] billed_month: {values}")

        return values

    def _aggregate_monthly_cost_data(
        self,
        data_source_id: str,
        domain_id: str,
        job_id: str,
        job_task_id: str,
        billed_month: str,
        data_keys: list,
        additional_info_keys: list,
        tag_keys: list,
    ):
        query = {
            "group_by": [
                "usage_unit",
                "provider",
                "region_code",
                "region_key",
                "product",
                "usage_type",
                "resource",
                # "tags",
                # "additional_info",
                "service_account_id",
                "project_id",
                "workspace_id",
                "billed_year",
            ],
            "fields": {
                "cost": {"key": "cost", "operator": "sum"},
                "usage_quantity": {"key": "usage_quantity", "operator": "sum"},
            },
            "start": billed_month,
            "end": billed_month,
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "job_id", "v": job_id, "o": "eq"},
                {"k": "job_task_id", "v": job_task_id, "o": "eq"},
            ],
            "allow_disk_use": True,  # Allow disk use for large data
            "return_type": "cursor",  # Return type is cursor
        }

        for info_key in additional_info_keys:
            query["group_by"].append(
                {
                    "key": f"additional_info.{info_key}",
                    "name": f"additional_info_{info_key}",
                }
            )

        # temporary remove tag group by
        # for tag_key in tag_keys:
        #     query["group_by"].append(
        #         {"key": f"tags.{tag_key}", "name": f"tags_{tag_key}"}
        #     )

        for data_key in data_keys:
            query["fields"].update(
                {f"data_{data_key}": {"key": f"data.{data_key}", "operator": "sum"}}
            )

        cursor = self.cost_mgr.analyze_costs(query, domain_id, target="PRIMARY")

        row_count = 0
        for row in cursor:
            aggregated_cost_data = copy.deepcopy(row)
            aggregated_cost_data["additional_info"] = {}
            aggregated_cost_data["tags"] = {}

            for key, value in row.get("_id", {}).items():
                if key.startswith("additional_info_"):
                    aggregated_cost_data["additional_info"][
                        key.replace("additional_info_", "")
                    ] = value
                elif key.startswith("tags_"):
                    aggregated_cost_data["tags"][key.replace("tags_", "")] = value
                else:
                    aggregated_cost_data[key] = value

            aggregated_cost_data["data_source_id"] = data_source_id
            aggregated_cost_data["billed_month"] = billed_month
            aggregated_cost_data["job_id"] = job_id
            aggregated_cost_data["job_task_id"] = job_task_id
            aggregated_cost_data["domain_id"] = domain_id
            aggregated_cost_data["data"] = {}

            for data_key in data_keys:
                aggregated_cost_data["data"][data_key] = aggregated_cost_data.get(
                    f"data_{data_key}", 0
                )

            self.cost_mgr.create_monthly_cost(aggregated_cost_data)
            row_count += 1

        _LOGGER.debug(
            f"[_aggregate_monthly_cost_data] create monthly costs ({billed_month}): {job_id} (count = {row_count})"
        )

    def _get_all_data_sources(self):
        return self.data_source_mgr.filter_data_sources(
            state="ENABLED", data_source_type="EXTERNAL"
        )

    def _check_duplicate_job(
        self, data_source_id: str, domain_id: str, this_job_vo: Job
    ):
        query = {
            "filter": [
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "workspace_id", "v": this_job_vo.workspace_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "status", "v": "IN_PROGRESS", "o": "eq"},
                {"k": "job_id", "v": this_job_vo.job_id, "o": "not"},
            ]
        }

        job_vos, total_count = self.job_mgr.list_jobs(query)

        duplicate_job_time = datetime.utcnow() - timedelta(minutes=10)

        for job_vo in job_vos:
            if job_vo.created_at >= duplicate_job_time:
                return True
            elif job_vo.options.get("sync_mode") == "MANUAL":
                return True
            else:
                self.job_mgr.change_canceled_status(job_vo)

        return False

    def _get_job_task_ids(self, job_id, domain_id):
        job_task_ids = []
        job_task_vos = self.job_task_mgr.filter_job_tasks(
            job_id=job_id, domain_id=domain_id
        )

        for job_task_vo in job_task_vos:
            job_task_ids.append(job_task_vo.job_task_id)

        return job_task_ids

    def _get_data_source_account_map(
        self,
        data_source_id: str,
        domain_id: str,
        workspace_id: str,
        resource_group: str,
    ) -> Dict[str, DataSourceAccount]:
        data_source_account_map = {}
        conditions = {
            "data_source_id": data_source_id,
            "domain_id": domain_id,
        }
        if resource_group == "WORKSPACE":
            conditions["workspace_id"] = workspace_id

        data_source_account_vos = (
            self.data_source_account_mgr.filter_data_source_accounts(**conditions)
        )

        for data_source_account_vo in data_source_account_vos:
            data_source_account_map[data_source_account_vo.account_id] = (
                data_source_account_vo
            )

        return data_source_account_map

    def _get_linked_accounts_from_data_source_vo(
        self,
        data_source_vo: DataSource,
        options: dict,
        secret_data: dict,
        schema: dict = None,
    ) -> list:
        linked_accounts = []

        use_account_routing = self._check_use_account_routing(data_source_vo)
        if not use_account_routing:
            return linked_accounts

        data_source_svc = self.locator.get_service("DataSourceService")

        data_source_id = data_source_vo.data_source_id
        domain_id = data_source_vo.domain_id

        accounts_info = self.ds_plugin_mgr.get_linked_accounts(
            options, secret_data, domain_id, schema
        )

        # Create data source account
        data_source_svc.create_data_source_account_with_data_source_vo(
            accounts_info, data_source_vo
        )

        # Connect data source account by metadata account connect polices
        data_source_account_vos = (
            self.data_source_account_mgr.filter_data_source_accounts(
                data_source_id=data_source_id, domain_id=domain_id
            )
        )
        for data_source_account_vo in data_source_account_vos:
            if not data_source_account_vo.workspace_id:
                data_source_account_vo = (
                    self.data_source_account_mgr.connect_account_by_data_source_vo(
                        data_source_account_vo, data_source_vo
                    )
                )

            linked_accounts.append(
                {
                    "data_source_id": data_source_account_vo.data_source_id,
                    "account_id": data_source_account_vo.account_id,
                    "name": data_source_account_vo.name,
                    "is_sync": data_source_account_vo.is_sync,
                    "v_workspace_id": data_source_account_vo.v_workspace_id,
                }
            )

        # Update data_source_account and connected_workspace count related to data_source
        self.data_source_mgr.update_data_source_account_and_connected_workspace_count_by_vo(
            data_source_vo
        )

        _LOGGER.debug(
            f"[_get_linked_accounts_from_data_source_vo] linked_accounts total count: {len(linked_accounts)} / {data_source_id}"
        )
        return linked_accounts

    def _update_data_source_is_synced(self, job_vo: Job) -> None:
        domain_id = job_vo.domain_id
        data_source_id = job_vo.data_source_id
        synced_accounts = job_vo.synced_accounts or []
        synced_account_ids = []

        for synced_account_vo in synced_accounts:
            data_source_account_vos = (
                self.data_source_account_mgr.filter_data_source_accounts(
                    data_source_id=data_source_id,
                    account_id=synced_account_vo.account_id,
                    domain_id=domain_id,
                )
            )

            if data_source_account_vos:
                self.data_source_account_mgr.update_data_source_account_by_vo(
                    {"is_sync": True},
                    data_source_account_vos[0],
                )
            synced_account_ids.append(synced_account_vo.account_id)
        _LOGGER.debug(
            f"[_update_data_source_account_sync_status] synced_account_ids: {synced_account_ids} / {data_source_id} {domain_id}"
        )

    @staticmethod
    def _get_start_last_synchronized_at(params):
        start = params.get("start")
        last_synchronized_at = utils.datetime_to_iso8601(
            params.get("last_synchronized_at")
        )
        return start, last_synchronized_at

    @staticmethod
    def _check_use_account_routing(data_source_vo: DataSource) -> bool:
        plugin_info = data_source_vo.plugin_info.to_dict() or {}
        metadata = plugin_info.get("metadata", {})

        if metadata.get("use_account_routing", False):
            return True

        return False
