import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.dashboard_model import Dashboard

_LOGGER = logging.getLogger(__name__)


class DashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_model: Dashboard = self.locator.get_model('Dashboard')

    def create_dashboard(self, params):
        def _rollback(dashboard_vo):
            _LOGGER.info(f'[create_dashboard._rollback] '
                         f'Delete dashboard : {dashboard_vo.name} '
                         f'({dashboard_vo.dashboard_id})')
            dashboard_vo.delete()

        dashboard_vo: Dashboard = self.dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, dashboard_vo)

        return dashboard_vo

    def update_dashboard(self, params):
        dashboard_vo: Dashboard = self.get_dashboard(params['dashboard_id'], params['domain_id'])
        return self.update_dashboard_by_vo(params, dashboard_vo)

    def update_dashboard_by_vo(self, params, dashboard_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_dashboard_by_vo._rollback] Revert Data : '
                         f'{old_data["dashboard_id"]}')
            dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, dashboard_vo.to_dict())
        return dashboard_vo.update(params)

    def delete_dashboard(self, dashboard_id, domain_id):
        dashboard_vo: Dashboard = self.get_dashboard(dashboard_id, domain_id)
        dashboard_vo.delete()

    def get_dashboard(self, dashboard_id, domain_id, only=None):
        return self.dashboard_model.get(dashboard_id=dashboard_id, domain_id=domain_id, only=only)

    def list_dashboards(self, query={}):
        return self.dashboard_model.query(**query)

    def stat_dashboards(self, query):
        return self.dashboard_model.stat(**query)
