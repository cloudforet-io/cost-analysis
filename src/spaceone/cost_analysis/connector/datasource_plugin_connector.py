import logging

from spaceone.core.connector import BaseConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil

__all__ = ["DataSourcePluginConnector"]

_LOGGER = logging.getLogger(__name__)


class DataSourcePluginConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.secret_data = None
        self.options = None
        self.schema = None
        token = self.transaction.get_meta("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")

    def initialize(self, endpoint):
        static_endpoint = self.config.get("endpoint")

        if static_endpoint:
            endpoint = static_endpoint

        self.client = self.locator.get_connector(
            "SpaceConnector", endpoint=endpoint, token="NO_TOKEN"
        )

        self.secret_data = self.config.get("secret_data")
        self.options = self.config.get("options")
        self.schema = self.config.get("schema")

    def init(self, options, domain_id):
        return self.client.dispatch(
            "DataSource.init", {"options": options, "domain_id": domain_id}
        )

    def verify(self, options, secret_data, schema, domain_id):
        params = {
            "options": self.options or options,
            "secret_data": self.secret_data or secret_data,
            "schema": self.schema or schema,
            "domain_id": domain_id,
        }

        self.client.dispatch("DataSource.verify", params)

    def get_tasks(
        self,
        options: dict,
        secret_data: dict,
        schema: str,
        domain_id: str,
        start: str = None,
        last_synchronized_at: str = None,
        linked_accounts: list = None,
    ):
        params = {
            "options": self.options or options,
            "secret_data": self.secret_data or secret_data,
            "schema": self.schema or schema,
            "start": start,
            "last_synchronized_at": last_synchronized_at,
            "domain_id": domain_id,
        }
        if linked_accounts:
            params["linked_accounts"] = linked_accounts

        return self.client.dispatch("Job.get_tasks", params)

    def get_linked_accounts(
        self, options: dict, secret_data: dict, domain_id: str, schema: dict = None
    ) -> dict:
        params = {
            "options": options,
            "secret_data": secret_data,
            "schema": schema,
            "domain_id": domain_id,
        }

        return self.client.dispatch("Cost.get_linked_accounts", params)

    def get_cost_data(self, options, secret_data, schema, task_options, domain_id):
        params = {
            "options": self.options or options,
            "secret_data": self.secret_data or secret_data,
            "schema": self.schema or schema,
            "task_options": task_options,
            "domain_id": domain_id,
        }
        return self.client.dispatch("Cost.get_data", params)
