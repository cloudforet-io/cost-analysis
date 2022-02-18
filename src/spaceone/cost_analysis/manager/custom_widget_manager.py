import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.custom_widget_model import CustomWidget

_LOGGER = logging.getLogger(__name__)


class CustomWidgetManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_widget_model: CustomWidget = self.locator.get_model('CustomWidget')

    def create_custom_widget(self, params):
        def _rollback(custom_widget_vo):
            _LOGGER.info(f'[create_custom_widget._rollback] '
                         f'Delete custom_widget : {custom_widget_vo.name} '
                         f'({custom_widget_vo.widget_id})')
            custom_widget_vo.delete()

        custom_widget_vo: CustomWidget = self.custom_widget_model.create(params)
        self.transaction.add_rollback(_rollback, custom_widget_vo)

        return custom_widget_vo

    def update_custom_widget(self, params):
        custom_widget_vo: CustomWidget = self.get_custom_widget(params['widget_id'], params['domain_id'])
        return self.update_custom_widget_by_vo(params, custom_widget_vo)

    def update_custom_widget_by_vo(self, params, custom_widget_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_custom_widget_by_vo._rollback] Revert Data : '
                         f'{old_data["widget_id"]}')
            custom_widget_vo.update(old_data)

        self.transaction.add_rollback(_rollback, custom_widget_vo.to_dict())
        return custom_widget_vo.update(params)

    def delete_custom_widget(self, widget_id, domain_id):
        custom_widget_vo: CustomWidget = self.get_custom_widget(widget_id, domain_id)
        custom_widget_vo.delete()

    def get_custom_widget(self, widget_id, domain_id, only=None):
        return self.custom_widget_model.get(widget_id=widget_id, domain_id=domain_id, only=only)

    def list_custom_widgets(self, query={}):
        return self.custom_widget_model.query(**query)

    def stat_custom_widgets(self, query):
        return self.custom_widget_model.stat(**query)
