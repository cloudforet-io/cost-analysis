import copy
import logging
import time
from typing import List, Union
from datetime import timedelta, datetime

from spaceone.core.service import *
from spaceone.core import cache, config, utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.job_task_model import JobTask
from spaceone.cost_analysis.model.job_model import Job
from spaceone.cost_analysis.model.data_source_model import DataSource
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.job_manager import JobManager
from spaceone.cost_analysis.manager.job_task_manager import JobTaskManager
from spaceone.cost_analysis.manager.data_source_plugin_manager import DataSourcePluginManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.manager.budget_usage_manager import BudgetUsageManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager('CostManager')
        self.job_mgr: JobManager = self.locator.get_manager('JobManager')
        self.job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['job_id', 'domain_id'])
    def cancel(self, params):
        """ Get job

        Args:
            params (dict): {
                'job_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            job_vo (object)
        """

        job_id = params['job_id']
        domain_id = params['domain_id']

        job_vo = self.job_mgr.get_job(job_id, domain_id)

        if job_vo.status != 'IN_PROGRESS':
            raise ERROR_JOB_STATE(job_state=job_vo.status)

        return self.job_mgr.change_canceled_status(job_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['job_id', 'domain_id'])
    def get(self, params):
        """ Get job

        Args:
            params (dict): {
                'job_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            job_vo (object)
        """

        job_id = params['job_id']
        domain_id = params['domain_id']

        return self.job_mgr.get_job(job_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['job_id', 'status', 'data_source_id', 'domain_id'])
    @append_keyword_filter(['job_id'])
    def list(self, params):
        """ List jobs

        Args:
            params (dict): {
                'job_id': 'str',
                'status': 'str',
                'data_source_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            job_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.job_mgr.list_jobs(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['job_id'])
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

        query = params.get('query', {})
        return self.job_mgr.stat_jobs(query)

    @transaction
    @check_required(['task_options', 'job_task_id', 'domain_id'])
    def get_cost_data(self, params):
        """Execute task to get cost data

        Args:
            params (dict): {
                'task_options': 'dict',
                'job_task_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager('DataSourcePluginManager')

        task_options = params['task_options']
        job_task_id = params['job_task_id']
        domain_id = params['domain_id']

        job_task_vo: JobTask = self.job_task_mgr.get_job_task(job_task_id, domain_id)

        job_id = job_task_vo.job_id

        if self._is_job_canceled(job_id, domain_id):
            self.job_task_mgr.change_canceled_status(job_task_vo)
        else:
            job_task_vo = self.job_task_mgr.change_in_progress_status(job_task_vo)

            try:
                data_source_vo: DataSource = data_source_mgr.get_data_source(job_task_vo.data_source_id, domain_id)
                plugin_info = data_source_vo.plugin_info.to_dict()

                secret_id = plugin_info.get('secret_id')
                options = plugin_info.get('options', {})
                schema = plugin_info.get('schema')

                endpoint, updated_version = ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)

                secret_data = self._get_secret_data(secret_id, domain_id)

                ds_plugin_mgr.initialize(endpoint)
                start_dt = datetime.utcnow()

                count = 0
                is_canceled = False
                _LOGGER.debug(f'[get_cost_data] start job ({job_task_id}): {start_dt}')
                for costs_data in ds_plugin_mgr.get_cost_data(options, secret_data, schema, task_options):
                    results = costs_data.get('results', [])
                    for cost_data in results:
                        count += 1

                        self._check_cost_data(cost_data)
                        self._create_cost_data(cost_data, job_task_vo)

                    if self._is_job_canceled(job_id, domain_id):
                        self.job_task_mgr.change_canceled_status(job_task_vo)
                        is_canceled = True
                    else:
                        job_task_vo = self.job_task_mgr.update_sync_status(job_task_vo, len(results))

                if is_canceled is False:
                    end_dt = datetime.utcnow()
                    _LOGGER.debug(f'[get_cost_data] end job ({job_task_id}): {end_dt}')
                    _LOGGER.debug(f'[get_cost_data] total job time ({job_task_id}): {end_dt - start_dt}')

                    self.job_task_mgr.change_success_status(job_task_vo, count)

            except Exception as e:
                self.job_task_mgr.change_error_status(job_task_vo, e)

        self._close_job(job_id, domain_id)

    def _get_secret_data(self, secret_id, domain_id):
        secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        if secret_id:
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
        else:
            secret_data = {}

        return secret_data

    @staticmethod
    def _check_cost_data(cost_data):
        if 'currency' not in cost_data:
            _LOGGER.error(f'[_check_cost_data] cost_data: {cost_data}')
            raise ERROR_REQUIRED_PARAMETER(key='plugin_cost_data.currency')

        if 'billed_at' not in cost_data:
            _LOGGER.error(f'[_check_cost_data] cost_data: {cost_data}')
            raise ERROR_REQUIRED_PARAMETER(key='plugin_cost_data.billed_at')

    def _create_cost_data(self, cost_data, job_task_vo):
        cost_data['original_currency'] = cost_data.get('currency', 'USD')
        cost_data['original_cost'] = cost_data.get('cost', 0)
        cost_data['job_id'] = job_task_vo.job_id
        cost_data['data_source_id'] = job_task_vo.data_source_id
        cost_data['domain_id'] = job_task_vo.domain_id
        cost_data['billed_at'] = utils.iso8601_to_datetime(cost_data['billed_at'])

        self.cost_mgr.create_cost(cost_data, execute_rollback=False)

    def _is_job_canceled(self, job_id, domain_id):
        job_vo: Job = self.job_mgr.get_job(job_id, domain_id)

        if job_vo.status == 'CANCELED':
            return True
        else:
            return False

    def _close_job(self, job_id, domain_id):
        job_vo: Job = self.job_mgr.get_job(job_id, domain_id)

        if job_vo.remained_tasks == 0:
            if job_vo.status == 'IN_PROGRESS':
                for changed_vo in job_vo.changed:
                    self._delete_changed_cost_data(job_vo, changed_vo.start, changed_vo.end)

                self._remove_cache(domain_id)
                self._update_last_sync_time(job_vo)
                self._update_budget_usage(domain_id)
                self.job_mgr.change_success_status(job_vo)

            elif job_vo.status == 'ERROR':
                self._rollback_cost_data(job_vo)
                self.job_mgr.update_job_by_vo({'finished_at': datetime.utcnow()}, job_vo)

            elif job_vo.status == 'CANCELED':
                self._rollback_cost_data(job_vo)

    def _update_budget_usage(self, domain_id):
        budget_mgr: BudgetManager = self.locator.get_manager('BudgetManager')
        budget_usage_mgr: BudgetUsageManager = self.locator.get_manager('BudgetUsageManager')
        budget_vos = budget_mgr.filter_budgets(domain_id=domain_id)
        for budget_vo in budget_vos:
            budget_usage_mgr.update_cost_usage(budget_vo)

    @staticmethod
    def _remove_cache(domain_id):
        cache.delete_pattern(f'stat-costs:{domain_id}:*')

    def _rollback_cost_data(self, job_vo: Job):
        cost_vos = self.cost_mgr.filter_costs(data_source_id=job_vo.data_source_id, domain_id=job_vo.domain_id,
                                              job_id=job_vo.job_id)

        _LOGGER.debug(f'[_close_job] delete cost data created by job: {job_vo.job_id} (count = {cost_vos.count()})')
        cost_vos.delete()

    def _update_last_sync_time(self, job_vo: Job):
        data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        data_source_vo = data_source_mgr.get_data_source(job_vo.data_source_id, job_vo.domain_id)
        data_source_mgr.update_data_source_by_vo({'last_synchronized_at': job_vo.created_at}, data_source_vo)

    def _delete_changed_cost_data(self, job_vo: Job, start, end):
        query = {
            'filter': [
                {'k': 'billed_at', 'v': start, 'o': 'gte'},
                {'k': 'data_source_id', 'v': job_vo.data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': job_vo.domain_id, 'o': 'eq'},
                {'k': 'job_id', 'v': job_vo.job_id, 'o': 'not'},
            ]
        }

        if end:
            query['filter'].append({'k': 'billed_at', 'v': end, 'o': 'lt'})

        _LOGGER.debug(f'[_delete_changed_cost_data] delete query: {query}')
        cost_vos, total_count = self.cost_mgr.list_costs(query)
        cost_vos.delete()

    def _aggregate_cost_data(self, job_vo: Job):
        query = {
            'aggregate': [
                {
                    'group': {
                        'keys': [
                            {'key': 'provider', 'name': 'provider'},
                            {'key': 'region_code', 'name': 'region_code'},
                            {'key': 'product', 'name': 'product'},
                            {'key': 'account', 'name': 'account'},
                            {'key': 'usage_type', 'name': 'usage_type'},
                            {'key': 'resource_group', 'name': 'resource_group'},
                            {'key': 'service_account_id', 'name': 'service_account_id'},
                            {'key': 'project_id', 'name': 'project_id'},
                            {'key': 'billed_at', 'name': 'billed_at', 'date_format': '%Y-%m-%d'},
                        ],
                        'fields': [
                            {'key': 'usd_cost', 'name': 'usd_cost', 'operator': 'sum'},
                            {'key': 'usage_quantity', 'name': 'usage_quantity', 'operator': 'sum'},
                        ]
                    }
                }
            ],
            'filter': [
                {'k': 'data_source_id', 'v': job_vo.data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': job_vo.domain_id, 'o': 'eq'},
                {'k': 'job_id', 'v': job_vo.job_id, 'o': 'eq'},
            ]
        }

        _LOGGER.debug(f'[_aggregate_cost_data] dump aggregated cost: {job_vo.job_id}')

        response = self.cost_mgr.stat_costs(query)
        results = response.get('results', [])
        for aggregated_cost_data in results:
            aggregated_cost_data['data_source_id'] = job_vo.data_source_id
            aggregated_cost_data['job_id'] = job_vo.job_id
            aggregated_cost_data['domain_id'] = job_vo.domain_id
            # self.cost_mgr.create_aggregate_cost_data(aggregated_cost_data)

        _LOGGER.debug(f'[_aggregate_cost_data] finished: {job_vo.job_id} (count = {len(results)})')
