import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core import utils
from spaceone.cost_analysis.error import *

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="secret", token=config.get_global("TOKEN")
        )

    def create_secret(
        self,
        secret_data: dict,
        schema_id: str,
        resource_group: str,
        workspace_id: str = None,
    ):
        def _rollback(secret_id: str):
            _LOGGER.info(f"[create_secret._rollback] Delete secret : {secret_id}")
            self.delete_secret(secret_id)

        params = {
            "name": utils.generate_id("secret-cost-data-source"),
            "data": secret_data,
            "schema_id": schema_id,
            "resource_group": resource_group,
        }

        if workspace_id:
            params["workspace_id"] = workspace_id

        token = self.transaction.get_meta("token")
        response = self.secret_connector.dispatch("Secret.create", params, token=token)

        _LOGGER.debug(f"[_create_secret] {response}")
        secret_id = response["secret_id"]

        self.transaction.add_rollback(_rollback, secret_id)

        return secret_id

    def delete_secret(self, secret_id: str):
        token = self.transaction.get_meta("token")
        self.secret_connector.dispatch(
            "Secret.delete", {"secret_id": secret_id}, token=token
        )

    def list_secrets(self, query: dict):
        token = self.transaction.get_meta("token")
        return self.secret_connector.dispatch(
            "Secret.list", {"query": query}, token=token
        )

    def get_secret(self, secret_id: str):
        token = self.transaction.get_meta("token")
        return self.secret_connector.dispatch(
            "Secret.get", {"secret_id": secret_id}, token=token
        )

    def get_secret_data(self, secret_id, domain_id):
        response = self.secret_connector.dispatch(
            "Secret.get_data", {"secret_id": secret_id, "domain_id": domain_id}
        )
        return response["data"]
