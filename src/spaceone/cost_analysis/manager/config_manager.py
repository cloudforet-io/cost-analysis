import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)

_AUTH_CONFIG_KEYS = ["settings"]


class ConfigManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="config"
        )

    def get_unified_cost_config(self, domain_id: str) -> dict:
        system_token = config.get_global("TOKEN")
        params = {
            "query": {
                "filter": [
                    {"k": "name", "v": _AUTH_CONFIG_KEYS, "o": "in"},
                ]
            }
        }

        response = self.list_domain_configs(
            params, token=system_token, x_domain_id=domain_id
        )

        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        if total_count > 0:
            domain_config_data = results[0].get("data", {})
            unified_cost_config = domain_config_data.get("unified_cost_config", {})
        else:
            domain_config_data = {}
            unified_cost_config = {}

        if not unified_cost_config:
            unified_cost_config = self._set_default_unified_cost_config(
                domain_id, data=domain_config_data
            )

        return unified_cost_config

    def set_domain_config(self, domain_id: str, name: str, data: dict) -> dict:
        system_token = config.get_global("TOKEN")
        params = {"name": name, "data": data}

        return self.config_conn.dispatch(
            "DomainConfig.set", params, token=system_token, x_domain_id=domain_id
        )

    def list_domain_configs(
        self, params: dict, token: str = None, x_domain_id: str = None
    ) -> dict:
        return self.config_conn.dispatch(
            "DomainConfig.list",
            params,
            token=token,
            x_domain_id=x_domain_id,
        )

    @staticmethod
    def _get_default_unified_cost_config() -> dict:
        default_unified_cost_config = {
            "run_hour": config.get_global("UNIFIED_COST_RUN_HOUR", 0),
            "aggregation_day": config.get_global("UNIFIED_COST_AGGREGATION_DAY", 15),
            "is_last_day": False,
            "exchange_source": "Yahoo! Finance",
            "exchange_date": 15,
            "is_exchange_last_day": False,
            "exchange_rate_mode": "AUTO",
            "custom_exchange_rate": {},
            "currency": "KRW",
        }
        return default_unified_cost_config

    def _set_default_unified_cost_config(self, domain_id: str, data: dict) -> dict:
        default_unified_cost_config = self._get_default_unified_cost_config()

        try:
            domain_config_name = _AUTH_CONFIG_KEYS[0]
            data["unified_cost_config"] = default_unified_cost_config

            domain_config_info = self.set_domain_config(
                domain_id, domain_config_name, data
            )

            default_unified_cost_config = domain_config_info["data"][
                "unified_cost_config"
            ]

        except Exception as e:
            _LOGGER.error(
                f"Failed to set default unified cost config: {e}", exc_info=True
            )

        return default_unified_cost_config
