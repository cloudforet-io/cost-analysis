import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)

_AUTH_CONFIG_KEYS = ["settings"]
_DEFAULT_UNIFIED_COST_CONFIG = {
    "run_hour": config.get_global("UNIFIED_COST_RUN_HOUR", 0),
    "aggregation_day": config.get_global("UNIFIED_COST_AGGREGATION_DAY", 15),
    "is_last_day": False,
    "exchange_source": "Yahoo Finance!",
    "exchange_date": 15,
    "is_exchange_last_day": False,
    "exchange_rate_mode": "AUTO",
    "custom_exchange_rate": {},
    "currency": "KRW",
}


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

        unified_cost_config = {}
        for config_info in response.get("results", []):
            if data := config_info.get("data", {}):
                unified_cost_config = data.get(
                    "unified_cost_config", _DEFAULT_UNIFIED_COST_CONFIG
                )
        if not unified_cost_config:
            unified_cost_config = _DEFAULT_UNIFIED_COST_CONFIG

        return unified_cost_config

    def list_domain_configs(
        self, params: dict, token: str = None, x_domain_id: str = None
    ) -> dict:
        return self.config_conn.dispatch(
            "DomainConfig.list",
            params,
            token=token,
            x_domain_id=x_domain_id,
        )
