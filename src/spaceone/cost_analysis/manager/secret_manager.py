import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core import utils
from spaceone.core.auth.jwt.jwt_util import JWTUtil

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.secret_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="secret"
        )

    def create_secret(
        self,
        secret_data: dict,
        resource_group: str,
        schema_id: str,
        workspace_id: str,
    ):
        def _rollback(secret_id: str):
            _LOGGER.info(f"[create_secret._rollback] Delete secret : {secret_id}")
            self.delete_secret(secret_id)

        params = {
            "name": utils.generate_id("secret-cost-data-source"),
            "resource_group": resource_group,
            "data": secret_data,
            "schema_id": schema_id,
            "workspace_id": workspace_id,
        }

        response = self.secret_connector.dispatch("Secret.create", params)

        _LOGGER.debug(f"[_create_secret] {response}")
        secret_id = response["secret_id"]

        self.transaction.add_rollback(_rollback, secret_id)

        return secret_id

    def delete_secret(self, secret_id: str):
        self.secret_connector.dispatch("Secret.delete", {"secret_id": secret_id})

    def list_secrets(self, query: dict, domain_id: str = None) -> dict:
        params = {"query": query}

        if self.token_type == "SYSTEM_TOKEN":
            return self.secret_connector.dispatch(
                "Secret.list", params, x_domain_id=domain_id
            )
        else:
            return self.secret_connector.dispatch("Secret.list", params)

    def get_secret(self, secret_id: str):
        return self.secret_connector.dispatch("Secret.get", {"secret_id": secret_id})

    def get_secret_data(self, secret_id, domain_id):
        system_token = config.get_global("TOKEN")

        response = self.secret_connector.dispatch(
            "Secret.get_data",
            {"secret_id": secret_id, "domain_id": domain_id},
            token=system_token,
        )
        return response["data"]
