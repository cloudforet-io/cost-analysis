import copy
import datetime
import logging
import time
from typing import List, Union
from dateutil import relativedelta
from datetime import timedelta, datetime

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.job_task_model import JobTask
from spaceone.cost_analysis.model.job_model import Job
from spaceone.cost_analysis.model.data_source_model import DataSource
from spaceone.cost_analysis.model.cost_model import CostQueryHistory
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
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager('DataSourcePluginManager')

    @transaction(append_meta={'authorization.scope': 'SYSTEM'})
    def create_jobs_by_data_source(self, params):
        """ Create jobs by domain

        Args:
            params (dict): {}

        Returns:
            None
        """

        for data_source_vo in self._get_all_data_sources():
            self._sync_data_source(data_source_vo)

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

    @transaction(append_meta={'authorization.scope': 'SYSTEM'})
    def preload_cache(self, params):
        """ Preload query results into cache

        Args:
            params (dict): {
                'query_hash': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        query_hash = params['query_hash']
        domain_id = params['domain_id']

        history_vos: List[CostQueryHistory] = self.cost_mgr.filter_cost_query_history(query_hash=query_hash,
                                                                                      domain_id=domain_id)
        for history_vo in history_vos:
            self._create_cache_by_history(history_vo, domain_id)
            _LOGGER.debug(f'[preload_cache] cache creation complete: {history_vo.query_hash}')

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
                data_source_vo: DataSource = self.data_source_mgr.get_data_source(job_task_vo.data_source_id, domain_id)
                plugin_info = data_source_vo.plugin_info.to_dict()

                secret_id = plugin_info.get('secret_id')
                options = plugin_info.get('options', {})
                schema = plugin_info.get('schema')

                endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)

                secret_data = self._get_secret_data(secret_id, domain_id)

                self.ds_plugin_mgr.initialize(endpoint)
                start_dt = datetime.utcnow()

                count = 0
                is_canceled = False
                _LOGGER.debug(f'[get_cost_data] start job ({job_task_id}): {start_dt}')
                for costs_data in self.ds_plugin_mgr.get_cost_data(options, secret_data, schema, task_options):
                    results = costs_data.get('results', [])
                    for cost_data in results:
                        count += 1

                        self._check_cost_data(cost_data)
                        self._create_cost_data(cost_data, job_task_vo)

                    if self._is_job_canceled(job_id, domain_id):
                        self.job_task_mgr.change_canceled_status(job_task_vo)
                        is_canceled = True
                        break
                    else:
                        job_task_vo = self.job_task_mgr.update_sync_status(job_task_vo, len(results))

                if not is_canceled:
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

                self.cost_mgr.remove_stat_cache(domain_id)
                self._update_last_sync_time(job_vo)
                self._update_budget_usage(domain_id)
                self.job_mgr.change_success_status(job_vo)
                self._preload_cost_stat_queries(domain_id)

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

    def _rollback_cost_data(self, job_vo: Job):
        cost_vos = self.cost_mgr.filter_costs(data_source_id=job_vo.data_source_id, domain_id=job_vo.domain_id,
                                              job_id=job_vo.job_id)

        _LOGGER.debug(f'[_close_job] delete cost data created by job: {job_vo.job_id} (count = {cost_vos.count()})')
        cost_vos.delete()

    def _update_last_sync_time(self, job_vo: Job):
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        data_source_vo = self.data_source_mgr.get_data_source(job_vo.data_source_id, job_vo.domain_id)
        self.data_source_mgr.update_data_source_by_vo({'last_synchronized_at': job_vo.created_at}, data_source_vo)

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

    def _sync_data_source(self, data_source_vo: DataSource):
        data_source_id = data_source_vo.data_source_id
        domain_id = data_source_vo.domain_id
        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(data_source_vo)
        secret_id = data_source_vo.plugin_info.secret_id
        options = data_source_vo.plugin_info.options
        schema = data_source_vo.plugin_info.schema
        secret_data = self._get_secret_data(secret_id, domain_id)

        _LOGGER.debug(f'[create_jobs_by_data_source] sync data source: {data_source_id}')

        params = {'last_synchronized_at': data_source_vo.last_synchronized_at}

        self.ds_plugin_mgr.initialize(endpoint)
        tasks, changed = self.ds_plugin_mgr.get_tasks(options, secret_data, schema, params)

        _LOGGER.debug(f'[sync] get_tasks: {tasks}')
        _LOGGER.debug(f'[sync] changed: {changed}')

        job_vo = self.job_mgr.create_job(data_source_id, domain_id, len(tasks), changed)

        if self._check_duplicate_job(data_source_id, domain_id, job_vo):
            self.job_mgr.change_error_status(job_vo, ERROR_DUPLICATE_JOB(data_source_id=data_source_id))
        else:
            if len(tasks) > 0:
                for task in tasks:
                    job_task_vo = None
                    task_options = task['task_options']
                    try:
                        job_task_vo = self.job_task_mgr.create_job_task(job_vo.job_id, data_source_id, domain_id,
                                                                        task_options)
                        self.job_task_mgr.push_job_task({
                            'task_options': task_options,
                            'job_task_id': job_task_vo.job_task_id,
                            'domain_id': domain_id
                        })
                    except Exception as e:
                        if job_task_vo:
                            self.job_task_mgr.change_error_status(job_task_vo, e)
            else:
                job_vo = self.job_mgr.change_success_status(job_vo)
                self.data_source_mgr.update_data_source_by_vo({'last_synchronized_at': job_vo.created_at},
                                                              data_source_vo)

    def _get_all_data_sources(self):
        return self.data_source_mgr.filter_data_sources(state='ENABLED', data_source_type='EXTERNAL')

    def _check_duplicate_job(self, data_source_id, domain_id, this_job_vo: Job):
        query = {
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'status', 'v': 'IN_PROGRESS', 'o': 'eq'},
                {'k': 'job_id', 'v': this_job_vo.job_id, 'o': 'not'},
            ]
        }

        job_vos, total_count = self.job_mgr.list_jobs(query)

        duplicate_job_time = datetime.utcnow() - timedelta(minutes=10)

        for job_vo in job_vos:
            if job_vo.created_at >= duplicate_job_time:
                return True
            else:
                self.job_mgr.change_canceled_status(job_vo)

        return False

    def _preload_cost_stat_queries(self, domain_id):
        cost_mgr: CostManager = self.locator.get_manager('CostManager')
        history_vos: List[CostQueryHistory] = cost_mgr.filter_cost_query_history(domain_id=domain_id)
        for history_vo in history_vos:
            self.job_task_mgr.push_preload_cache_task({'query_hash': history_vo.query_hash, 'domain_id': domain_id})

    def _create_cache_by_history(self, history_vo: CostQueryHistory, domain_id):
        query = history_vo.query
        original_start = history_vo.start
        original_end = history_vo.end
        this_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        this_month_end = this_month_start + relativedelta.relativedelta(months=1)

        # Original Date Range
        self._create_cache(copy.deepcopy(query), original_start, original_end, domain_id)

        # This month
        self._create_cache(copy.deepcopy(query), this_month_start, this_month_end, domain_id)

        # Last Month
        start = this_month_start - relativedelta.relativedelta(months=1)
        self._create_cache(copy.deepcopy(query), start, this_month_start, domain_id)

        # 2 Month Ago
        start = this_month_start - relativedelta.relativedelta(months=2)
        end = this_month_start - relativedelta.relativedelta(months=1)
        self._create_cache(copy.deepcopy(query), start, end, domain_id)

        # Last 3 Month
        start = this_month_start - relativedelta.relativedelta(months=2)
        self._create_cache(copy.deepcopy(query), start, this_month_end, domain_id)

        # Last 4 Month
        start = this_month_start - relativedelta.relativedelta(months=3)
        self._create_cache(copy.deepcopy(query), start, this_month_end, domain_id)

        # Last 6 Month
        start = this_month_start - relativedelta.relativedelta(months=5)
        self._create_cache(copy.deepcopy(query), start, this_month_end, domain_id)

        # Last 12 Month
        start = this_month_start - relativedelta.relativedelta(months=11)
        self._create_cache(copy.deepcopy(query), start, this_month_end, domain_id)

        # Last Month - 3 Month
        start = this_month_start - relativedelta.relativedelta(months=3)
        self._create_cache(copy.deepcopy(query), start, this_month_start, domain_id)

        # Last Month - 4 Month
        start = this_month_start - relativedelta.relativedelta(months=4)
        self._create_cache(copy.deepcopy(query), start, this_month_start, domain_id)

        # Last Month - 6 Month
        start = this_month_start - relativedelta.relativedelta(months=6)
        self._create_cache(copy.deepcopy(query), start, this_month_start, domain_id)

        # Last Month - 12 Month
        start = this_month_start - relativedelta.relativedelta(months=12)
        self._create_cache(copy.deepcopy(query), start, this_month_start, domain_id)

    def _create_cache(self, query, start, end, domain_id):
        query = self._add_date_range_filter(query, start, end)
        query_hash = utils.dict_to_hash(query)
        self.cost_mgr.stat_costs_with_cache(query, query_hash, domain_id)

    @staticmethod
    def _add_date_range_filter(query, start, end):
        query['filter'] = query.get('filter') or []

        if start:
            query['filter'].append({
                'k': 'billed_at',
                'v': utils.datetime_to_iso8601(start),
                'o': 'datetime_gte'
            })

        if end:
            query['filter'].append({
                'k': 'billed_at',
                'v': utils.datetime_to_iso8601(end),
                'o': 'datetime_lt'
            })

        return query
