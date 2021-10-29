import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.schedule_model import Schedule

_LOGGER = logging.getLogger(__name__)


class ScheduleManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schedule_model: Schedule = self.locator.get_model('Schedule')

    def create_schedule(self, params):
        def _rollback(schedule_vo):
            _LOGGER.info(f'[create_schedule._rollback] '
                         f'Delete schedule : {schedule_vo.name} '
                         f'({schedule_vo.schedule_id})')
            schedule_vo.delete()

        schedule_vo: Schedule = self.schedule_model.create(params)
        self.transaction.add_rollback(_rollback, schedule_vo)

        return schedule_vo

    def update_schedule(self, params):
        schedule_vo: Schedule = self.get_schedule(params['schedule_id'], params['domain_id'])
        return self.update_schedule_by_vo(params, schedule_vo)

    def update_schedule_by_vo(self, params, schedule_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_schedule_by_vo._rollback] Revert Data : '
                         f'{old_data["schedule_id"]}')
            schedule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, schedule_vo.to_dict())
        return schedule_vo.update(params)

    def delete_schedule(self, schedule_id, domain_id):
        schedule_vo: Schedule = self.get_schedule(schedule_id, domain_id)
        schedule_vo.delete()

    def get_schedule(self, schedule_id, domain_id, only=None):
        return self.schedule_model.get(schedule_id=schedule_id, domain_id=domain_id, only=only)

    def list_schedules(self, query={}):
        return self.schedule_model.query(**query)

    def stat_schedules(self, query):
        return self.schedule_model.stat(**query)
