import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil

_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.plugin_connector: SpaceConnector = self.locator.get_connector(
            SpaceConnector, service="plugin", token=config.get_global("TOKEN")
        )

    def get_plugin_endpoint(self, plugin_info, domain_id):
        if self.token_type == "SYSTEM_TOKEN":
            response = self.plugin_connector.dispatch(
                "Plugin.get_plugin_endpoint",
                {
                    "plugin_id": plugin_info["plugin_id"],
                    "version": plugin_info.get("version"),
                    "upgrade_mode": plugin_info.get("upgrade_mode", "AUTO"),
                    "domain_id": domain_id,
                },
                x_domain_id=domain_id,
            )
        else:
            response = self.plugin_connector.dispatch(
                "Plugin.get_plugin_endpoint",
                {
                    "plugin_id": plugin_info["plugin_id"],
                    "version": plugin_info.get("version"),
                    "upgrade_mode": plugin_info.get("upgrade_mode", "AUTO"),
                    "domain_id": domain_id,
                },
            )
        return response["endpoint"], response.get("updated_version")
