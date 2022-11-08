import copy
import datetime
import logging
from dateutil import rrule
from datetime import timedelta, datetime

from spaceone.core.service import *
from spaceone.core import utils, config
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
            try:
                self._sync_data_source(data_source_vo)
            except Exception as e:
                _LOGGER.error(f'[create_jobs_by_data_source] sync error: {e}', exc_info=True)

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
                for costs_data in self.ds_plugin_mgr.get_cost_data(options, secret_data, schema, task_options, domain_id):
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
        no_preload_cache = job_vo.options.get('no_preload_cache', False)

        if job_vo.remained_tasks == 0:
            if job_vo.status == 'IN_PROGRESS':
                try:
                    changed_start = None
                    for changed_vo in job_vo.changed:
                        self._delete_changed_cost_data(job_vo, changed_vo.start, changed_vo.end, changed_vo.filter)
                        if changed_start is None or changed_start > changed_vo.start:
                            changed_start = changed_vo.start

                    self._aggregate_cost_data(job_vo, changed_start)
                    self._update_budget_usage(domain_id)
                    self.cost_mgr.remove_stat_cache(domain_id)

                    if not no_preload_cache:
                        self._preload_cost_stat_queries(domain_id)

                    self._update_last_sync_time(job_vo)
                    self.job_mgr.change_success_status(job_vo)
                except Exception as e:
                    self.job_mgr.change_error_status(job_vo, e)
                    raise e

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

    def _delete_changed_cost_data(self, job_vo: Job, start, end, _filter):
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

        for key, value in _filter.items():
            query['filter'].append({'k': key, 'v': value, 'o': 'eq'})

        _LOGGER.debug(f'[_delete_changed_cost_data] query: {query}')
        cost_vos, total_count = self.cost_mgr.list_costs(query)
        cost_vos.delete()
        _LOGGER.debug(f'[_delete_changed_cost_data] delete costs (count = {total_count})')

    def _aggregate_cost_data(self, job_vo: Job, changed_start):
        data_source_id = job_vo.data_source_id
        domain_id = job_vo.domain_id
        job_id = job_vo.job_id
        changed_start = changed_start.replace(day=1)

        for dt in rrule.rrule(rrule.MONTHLY, dtstart=changed_start, until=datetime.utcnow()):
            billed_month = dt.strftime('%Y-%m')
            accounts = self._list_accounts_from_cost_data(data_source_id, domain_id, billed_month)

            for account in accounts:
                if self._is_large_data(data_source_id, domain_id, billed_month, account):
                    products = self._list_products_from_cost_data(data_source_id, domain_id, billed_month, account)

                    for product in products:
                        self._aggregate_monthly_cost_data(data_source_id, domain_id, job_id, billed_month, account,
                                                          product)

                else:
                    self._aggregate_monthly_cost_data(data_source_id, domain_id, job_id, billed_month, account)

        self._delete_aggregated_cost_data(data_source_id, domain_id, job_id, changed_start)

    def _list_accounts_from_cost_data(self, data_source_id, domain_id, billed_month):
        query = {
            'distinct': 'account',
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'billed_month', 'v': billed_month, 'o': 'eq'},
            ],
            'target': 'PRIMARY'  # Execute a query to primary DB
        }
        _LOGGER.debug(f'[_list_accounts_from_cost_data] query: {query}')
        response = self.cost_mgr.stat_costs(query)
        accounts = response.get('results', [])

        _LOGGER.debug(f'[_list_accounts_from_cost_data] accounts: {accounts}')

        return accounts

    def _list_products_from_cost_data(self, data_source_id, domain_id, billed_month, account):
        query = {
            'distinct': 'product',
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'billed_month', 'v': billed_month, 'o': 'eq'},
                {'k': 'account', 'v': account, 'o': 'eq'},
            ],
            'target': 'PRIMARY'  # Execute a query to primary DB
        }
        _LOGGER.debug(f'[_list_products_from_cost_data] query: {query}')
        response = self.cost_mgr.stat_costs(query)
        products = response.get('results', [])

        _LOGGER.debug(f'[_list_products_from_cost_data] products: {products}')

        return products

    def _is_large_data(self, data_source_id, domain_id, billed_month, account):
        query = {
            'count_only': True,
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'billed_month', 'v': billed_month, 'o': 'eq'},
                {'k': 'account', 'v': account, 'o': 'eq'},
            ],
            'target': 'PRIMARY'  # Execute a query to primary DB
        }
        cost_vos, total_count = self.cost_mgr.list_costs(query)

        _LOGGER.debug(f'[_is_large_data] cost count ({billed_month}): {total_count} => {total_count >= 100000}')

        # Split query by product if cost count exceeds 100k
        if total_count >= 100000:
            return True
        else:
            return False

    def _aggregate_monthly_cost_data(self, data_source_id, domain_id, job_id, billed_month, account, product=None):
        query = {
            'aggregate': [
                {
                    'group': {
                        'keys': [
                            {'key': 'provider', 'name': 'provider'},
                            {'key': 'region_code', 'name': 'region_code'},
                            {'key': 'region_key', 'name': 'region_key'},
                            {'key': 'category', 'name': 'category'},
                            {'key': 'product', 'name': 'product'},
                            {'key': 'account', 'name': 'account'},
                            {'key': 'usage_type', 'name': 'usage_type'},
                            {'key': 'resource_group', 'name': 'resource_group'},
                            {'key': 'resource', 'name': 'resource'},
                            {'key': 'tags', 'name': 'tags'},
                            {'key': 'additional_info', 'name': 'additional_info'},
                            {'key': 'service_account_id', 'name': 'service_account_id'},
                            {'key': 'project_id', 'name': 'project_id'},
                            {'key': 'data_source_id', 'name': 'data_source_id'},
                            {'key': 'billed_month', 'name': 'billed_month'},
                            {'key': 'billed_year', 'name': 'billed_year'},
                        ],
                        'fields': [
                            {'key': 'usd_cost', 'name': 'usd_cost', 'operator': 'sum'},
                            {'key': 'usage_quantity', 'name': 'usage_quantity', 'operator': 'sum'},
                        ]
                    }
                }
            ],
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'billed_month', 'v': billed_month, 'o': 'eq'},
                {'k': 'account', 'v': account, 'o': 'eq'},
            ],
            'target': 'PRIMARY'  # Execute a query to primary DB
        }

        if product:
            query['filter'].append({'k': 'product', 'v': product, 'o': 'eq'})

        _LOGGER.debug(f'[_aggregate_monthly_cost_data] query: {query}')
        response = self.cost_mgr.stat_costs(query)
        results = response.get('results', [])
        for aggregated_cost_data in results:
            aggregated_cost_data['data_source_id'] = data_source_id
            aggregated_cost_data['job_id'] = job_id
            aggregated_cost_data['domain_id'] = domain_id
            self.cost_mgr.create_monthly_cost(aggregated_cost_data)

        _LOGGER.debug(f'[_aggregate_monthly_cost_data] create monthly costs ({billed_month}): {job_id} (count = {len(results)})')

    def _delete_aggregated_cost_data(self, data_source_id, domain_id, job_id, changed_start):
        changed_start_month = changed_start.strftime('%Y-%m')
        changed_start_year = changed_start.strftime('%Y')

        # Delete Monthly Cost
        query = {
            'filter': [
                {'k': 'data_source_id', 'v': data_source_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'job_id', 'v': job_id, 'o': 'not'},
                {'k': 'billed_month', 'v': changed_start_month, 'o': 'gte'},
            ]
        }

        _LOGGER.debug(f'[_delete_aggregated_cost_data] query: {query}')
        monthly_cost_vos, total_count = self.cost_mgr.list_monthly_costs(query)
        monthly_cost_vos.delete()

        _LOGGER.debug(f'[_delete_aggregated_cost_data] delete monthly costs after {changed_start_month}: {job_id}')

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
        tasks, changed = self.ds_plugin_mgr.get_tasks(options, secret_data, schema, params, domain_id)

        _LOGGER.debug(f'[sync] get_tasks: {tasks}')
        _LOGGER.debug(f'[sync] changed: {changed}')

        # Add Job Options
        job_vo = self.job_mgr.create_job(data_source_id, domain_id, {}, len(tasks), changed)

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
        cost_query_cache_time = config.get_global('COST_QUERY_CACHE_TIME', 7)
        cache_time = datetime.utcnow() - timedelta(days=cost_query_cache_time)

        query = {
            'filter': [
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'updated_at', 'v': cache_time, 'o': 'gte'},
            ]
        }

        _LOGGER.debug(f'[_preload_cost_stat_queries] cost_query_cache_time: {cost_query_cache_time} days')

        history_vos, total_count = self.cost_mgr.list_cost_query_history(query)
        for history_vo in history_vos:
            _LOGGER.debug(f'[_preload_cost_stat_queries] create query cache: {history_vo.query_hash}')
            self._create_cache_by_history(history_vo, domain_id)

    def _create_cache_by_history(self, history_vo: CostQueryHistory, domain_id):
        query = history_vo.query_options
        granularity = history_vo.granularity
        start = history_vo.start
        end = history_vo.end

        # Original Date Range
        self._create_cache(copy.deepcopy(query), granularity, start, end, domain_id)

    def _create_cache(self, query, granularity, start, end, domain_id):
        query = self.cost_mgr.add_date_range_filter(query, granularity, start, end)
        query_hash_with_date_range = utils.dict_to_hash(query)

        if self.cost_mgr.is_monthly_cost(granularity, start, end):
            self.cost_mgr.stat_monthly_costs_with_cache(query, query_hash_with_date_range, domain_id, target='PRIMARY')
        else:
            self.cost_mgr.stat_costs_with_cache(query, query_hash_with_date_range, domain_id, target='PRIMARY')
