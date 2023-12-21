import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            SpaceConnector, service="identity", token=config.get_global("TOKEN")
        )

    def check_workspace(self, workspace_id: str, domain_id: str) -> None:
        self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
        )

    def get_workspace(self, workspace_id: str) -> dict:
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "Workspace.get", {"workspace_id": workspace_id}, token=token
        )

    def get_trusted_account(self, trusted_account_id: str) -> dict:
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "TrustedAccount.get",
            {"trusted_account_id": trusted_account_id},
            token=token,
        )

    def list_trusted_accounts(self, query: dict):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "TrustedAccount.list", {"query": query}, token=token
        )

    def get_service_account(self, service_account_id: str):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "ServiceAccount.get",
            {"service_account_id": service_account_id},
            token=token,
        )

    def list_service_accounts(self, query: dict, domain_id: str = None) -> dict:
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "ServiceAccount.list", {"query": query}, token=token, x_domain_id=domain_id
        )

    def get_project(self, project_id: str):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "Project.get", {"project_id": project_id}, token=token
        )

    def list_projects(self, query: dict):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "Project.list", {"query": query}, token=token
        )
