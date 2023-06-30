import logging
import copy
from typing import List
from datetime import datetime, timedelta

from spaceone.core.error import *
from spaceone.core import queue, utils, config
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.job_model import Job
from spaceone.cost_analysis.model.cost_model import CostQueryHistory
from spaceone.cost_analysis.manager.cost_manager import CostManager

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_model: Job = self.locator.get_model('Job')
        self.job_timeout = config.get_global('JOB_TIMEOUT', 7200)
        self.cost_mgr: CostManager = self.locator.get_manager('CostManager')

    def is_job_running(self, data_source_id, domain_id):
        job_vos: List[Job] = self.job_model.filter(data_source_id=data_source_id, domain_id=domain_id,
                                                   status='IN_PROGRESS')

        running_job_count = job_vos.count()

        for job_vo in job_vos:
            if datetime.utcnow() > (job_vo.created_at + timedelta(seconds=self.job_timeout)):
                self.change_timeout_status(job_vo)
                running_job_count -= 1

        if running_job_count > 0:
            return True
        else:
            return False

    def create_job(self, data_source_id, domain_id, job_options, total_tasks, changed=None):
        job_options['no_preload_cache'] = job_options.get('no_preload_cache', False)
        job_options['start'] = job_options.get('start')

        data = {
            'data_source_id': data_source_id,
            'domain_id': domain_id,
            'options': job_options
        }

        if total_tasks:
            data.update({
                'total_tasks': total_tasks,
                'remained_tasks': total_tasks
            })

        if changed:
            data['changed'] = []
            for changed_info in changed:
                start = utils.iso8601_to_datetime(changed_info['start'])
                end = utils.iso8601_to_datetime(changed_info['end']) if 'end' in changed_info else None
                _filter = changed_info.get('filter', {})

                data['changed'].append({
                    'start': start,
                    'end': end,
                    'filter': _filter
                })

        job_vo = self.job_model.create(data)

        _LOGGER.debug(f'[create_job] create job: {job_vo.job_id}')
        return job_vo

    def update_job_by_vo(self, params, job_vo):
        return job_vo.update(params)

    def get_job(self, job_id, domain_id, only=None):
        return self.job_model.get(job_id=job_id, domain_id=domain_id, only=only)

    def filter_jobs(self, **conditions):
        return self.job_model.filter(**conditions)

    def list_jobs(self, query={}):
        return self.job_model.query(**query)

    def stat_jobs(self, query):
        return self.job_model.stat(**query)

    def preload_cost_stat_queries(self, domain_id):
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
        query_hash = history_vo.query_hash
        granularity = history_vo.granularity
        start = history_vo.start
        end = history_vo.end

        # Original Date Range
        self._create_cache(domain_id, copy.deepcopy(query), query_hash, granularity, start, end)

    def _create_cache(self, domain_id, query, query_hash, granularity, start, end):
        if granularity and start and end:
            query = self.cost_mgr.add_date_range_filter(query, granularity, start, end)
            query_hash_with_date_range = utils.dict_to_hash(query)

            _LOGGER.debug(f'[_create_cache] query: {query}')

            if 'group_by' in query:
                if self.cost_mgr.is_monthly_cost(granularity, start, end):
                    self.cost_mgr.analyze_monthly_costs_with_cache(query, query_hash, domain_id)
                else:
                    self.cost_mgr.analyze_costs_with_cache(query, query_hash, domain_id)
            else:
                if self.cost_mgr.is_monthly_cost(granularity, start, end):
                    self.cost_mgr.stat_monthly_costs_with_cache(query, query_hash_with_date_range, domain_id,
                                                                target='PRIMARY')
                else:
                    self.cost_mgr.stat_costs_with_cache(query, query_hash_with_date_range, domain_id, target='PRIMARY')
        else:
            self.cost_mgr.stat_monthly_costs_with_cache(query, query_hash, domain_id, target='PRIMARY')

    @staticmethod
    def decrease_remained_tasks(job_vo: Job):
        return job_vo.decrement('remained_tasks', 1)

    @staticmethod
    def change_success_status(job_vo: Job):
        _LOGGER.info(f'[change_success_status] job success: {job_vo.job_id}')

        return job_vo.update({
            'status': 'SUCCESS',
            'finished_at': datetime.utcnow()
        })

    @staticmethod
    def change_canceled_status(job_vo: Job):
        _LOGGER.error(f'[change_canceled_status], job canceled ({job_vo.job_id})')

        return job_vo.update({
            'status': 'CANCELED',
            'finished_at': datetime.utcnow()
        })

    @staticmethod
    def change_timeout_status(job_vo: Job):
        _LOGGER.error(f'[change_timeout_status] job timeout: {job_vo.job_id}')

        return job_vo.update({
            'status': 'TIMEOUT',
            'finished_at': datetime.utcnow()
        })

    @staticmethod
    def change_error_status(job_vo: Job, e):
        if not isinstance(e, ERROR_BASE):
            e = ERROR_UNKNOWN(message=str(e))

        _LOGGER.error(f'[change_error_status] job error ({job_vo.job_id}): {e.message}', exc_info=True)

        job_vo.update({
            'status': 'FAILURE',
            'error_code': e.error_code,
            'error_message': e.message,
            'finished_at': datetime.utcnow()
        })
