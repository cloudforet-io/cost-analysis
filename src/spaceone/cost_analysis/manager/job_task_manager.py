import logging
from datetime import datetime

from spaceone.core import queue, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.job_manager import JobManager
from spaceone.cost_analysis.model.job_task_model import JobTask

_LOGGER = logging.getLogger(__name__)


class JobTaskManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.job_mgr: JobManager = self.locator.get_manager('JobManager')
        self.job_task_model: JobTask = self.locator.get_model('JobTask')

    def create_job_task(self, job_id, data_source_id, domain_id, task_options):
        data = {
            'job_id': job_id,
            'data_source_id': data_source_id,
            'domain_id': domain_id,
            'options': task_options
        }

        _LOGGER.debug(f'[create_job_task] create job task: {data}')

        return self.job_task_model.create(data)

    def get_job_task(self, job_task_id, domain_id, only=None):
        return self.job_task_model.get(job_task_id=job_task_id, domain_id=domain_id, only=only)

    def filter_job_tasks(self, **conditions):
        return self.job_task_model.filter(**conditions)

    def list_job_tasks(self, query={}):
        return self.job_task_model.query(**query)

    def stat_job_tasks(self, query):
        return self.job_task_model.stat(**query)

    def push_job_task(self, params):
        task = {
            'name': 'sync_data_source',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [{
                'locator': 'SERVICE',
                'name': 'JobService',
                'metadata': self.transaction.meta,
                'method': 'get_cost_data',
                'params': {
                    'params': params
                }
            }]
        }

        _LOGGER.debug(f'[push_job_task] task: {params}')

        queue.put('cost_analysis_q', utils.dump_json(task))

    @staticmethod
    def change_in_progress_status(job_task_vo: JobTask):
        _LOGGER.debug(f'[change_in_progress_status] start job task: {job_task_vo.job_task_id}')

        return job_task_vo.update({
            'status': 'IN_PROGRESS',
            'started_at': datetime.utcnow()
        })

    @staticmethod
    def update_sync_status(job_task_vo: JobTask, created_count):
        return job_task_vo.update({
            'created_count': job_task_vo.created_count + created_count
        })

    def change_success_status(self, job_task_vo: JobTask, created_count):
        _LOGGER.debug(f'[change_success_status] success job task: {job_task_vo.job_task_id} '
                      f'(created_count = {created_count})')

        job_task_vo.update({
            'status': 'SUCCESS',
            'created_count': created_count,
            'finished_at': datetime.utcnow()
        })

        job_vo = self.job_mgr.get_job(job_task_vo.job_id, job_task_vo.domain_id)
        self.job_mgr.decrease_remained_tasks(job_vo)

    def change_canceled_status(self, job_task_vo: JobTask):
        _LOGGER.error(f'[change_canceled_status], job task canceled ({job_task_vo.job_task_id})')

        job_task_vo.update({
            'status': 'CANCELED',
            'finished_at': datetime.utcnow()
        })

        job_vo = self.job_mgr.get_job(job_task_vo.job_id, job_task_vo.domain_id)
        self.job_mgr.decrease_remained_tasks(job_vo)

    def change_error_status(self, job_task_vo: JobTask, e):
        if not isinstance(e, ERROR_BASE):
            e = ERROR_UNKNOWN(message=str(e))

        _LOGGER.error(f'[change_error_status], error job task ({job_task_vo.job_task_id}): {e.message}', exc_info=True)

        job_task_vo.update({
            'status': 'FAILURE',
            'error_code': e.error_code,
            'error_message': e.message,
            'finished_at': datetime.utcnow()
        })

        job_vo = self.job_mgr.get_job(job_task_vo.job_id, job_task_vo.domain_id)
        self.job_mgr.change_error_status(job_vo, ERROR_JOB_TASK())
        self.job_mgr.decrease_remained_tasks(job_vo)
