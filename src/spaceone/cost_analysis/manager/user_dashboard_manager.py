import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.user_dashboard_model import UserDashboard

_LOGGER = logging.getLogger(__name__)


class UserDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_dashboard_model: UserDashboard = self.locator.get_model('UserDashboard')

    def create_user_dashboard(self, params):
        def _rollback(user_dashboard_vo):
            _LOGGER.info(f'[create_user_dashboard._rollback] '
                         f'Delete user_dashboard : {user_dashboard_vo.name} '
                         f'({user_dashboard_vo.user_dashboard_id})')
            user_dashboard_vo.delete()

        user_dashboard_vo: UserDashboard = self.user_dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, user_dashboard_vo)

        return user_dashboard_vo

    def update_user_dashboard(self, params):
        user_dashboard_vo: UserDashboard = self.get_user_dashboard(params['user_dashboard_id'], params['domain_id'])
        return self.update_user_dashboard_by_vo(params, user_dashboard_vo)

    def update_user_dashboard_by_vo(self, params, user_dashboard_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_user_dashboard_by_vo._rollback] Revert Data : '
                         f'{old_data["user_dashboard_id"]}')
            user_dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, user_dashboard_vo.to_dict())
        return user_dashboard_vo.update(params)

    def delete_user_dashboard(self, user_dashboard_id, domain_id):
        user_dashboard_vo: UserDashboard = self.get_user_dashboard(user_dashboard_id, domain_id)
        user_dashboard_vo.delete()

    def get_user_dashboard(self, user_dashboard_id, domain_id, only=None):
        return self.user_dashboard_model.get(user_dashboard_id=user_dashboard_id, domain_id=domain_id, only=only)

    def list_user_dashboards(self, query={}):
        return self.user_dashboard_model.query(**query)

    def stat_user_dashboards(self, query):
        return self.user_dashboard_model.stat(**query)
