import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class NotificationManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.notification_connector = SpaceConnector(service="notification", token=config.get_global("TOKEN"))

    def create_notification(self, message):
        _LOGGER.debug(f"Notify message: {message}")
        return self.notification_connector.dispatch("Notification.create", message)
