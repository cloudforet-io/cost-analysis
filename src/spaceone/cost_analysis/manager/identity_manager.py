import logging

from spaceone.core import cache
from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            SpaceConnector, service="identity", token=config.get_global("TOKEN")
        )

    def check_workspace(self, workspace_id: str, domain_id: str) -> None:
        system_token = config.get_global("TOKEN")

        self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    @cache.cacheable(key="workspace-name:{domain_id}:{workspace_id}:name", expire=300)
    def get_workspace_name_with_system_token(
        self, workspace_id: str, domain_id: str
    ) -> str:
        try:
            workspace_info = self.identity_conn.dispatch(
                "Workspace.get",
                {"workspace_id": workspace_id},
                x_domain_id=domain_id,
            )
            return workspace_info["name"]
        except Exception as e:
            _LOGGER.error(f"[get_project_name] API Error: {e}")
            return workspace_id

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

    @cache.cacheable(
        key="project-name:{domain_id}:{workspace_id}:{project_id}", expire=300
    )
    # workspace_id, domain_id, remain for cache
    def get_project_name(self, project_id: str, workspace_id: str, domain_id: str):
        try:
            project_info = self.get_project(project_id)
            return project_info["name"]
        except Exception as e:
            _LOGGER.error(f"[get_project_name] API Error: {e}")
            return project_id

    def get_project(self, project_id: str, domain_id: str = None):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "Project.get", {"project_id": project_id}, token=token
        )

    def list_projects(self, query: dict, domain_id: str = None):
        token = self.transaction.get_meta("token")
        return self.identity_conn.dispatch(
            "Project.list", {"query": query}, token=token, x_domain_id=domain_id
        )
