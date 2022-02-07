import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.public_dashboard_model import PublicDashboard

_LOGGER = logging.getLogger(__name__)


class PublicDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.public_dashboard_model: PublicDashboard = self.locator.get_model('PublicDashboard')

    def create_public_dashboard(self, params):
        def _rollback(public_dashboard_vo):
            _LOGGER.info(f'[create_public_dashboard._rollback] '
                         f'Delete public_dashboard : {public_dashboard_vo.name} '
                         f'({public_dashboard_vo.public_dashboard_id})')
            public_dashboard_vo.delete()

        public_dashboard_vo: PublicDashboard = self.public_dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, public_dashboard_vo)

        return public_dashboard_vo

    def update_public_dashboard(self, params):
        public_dashboard_vo: PublicDashboard = self.get_public_dashboard(params['public_dashboard_id'], params['domain_id'])
        return self.update_public_dashboard_by_vo(params, public_dashboard_vo)

    def update_public_dashboard_by_vo(self, params, public_dashboard_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_public_dashboard_by_vo._rollback] Revert Data : '
                         f'{old_data["public_dashboard_id"]}')
            public_dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, public_dashboard_vo.to_dict())
        return public_dashboard_vo.update(params)

    def delete_public_dashboard(self, public_dashboard_id, domain_id):
        public_dashboard_vo: PublicDashboard = self.get_public_dashboard(public_dashboard_id, domain_id)
        public_dashboard_vo.delete()

    def get_public_dashboard(self, public_dashboard_id, domain_id, only=None):
        return self.public_dashboard_model.get(public_dashboard_id=public_dashboard_id, domain_id=domain_id, only=only)

    def list_public_dashboards(self, query={}):
        return self.public_dashboard_model.query(**query)

    def stat_public_dashboards(self, query):
        return self.public_dashboard_model.stat(**query)
