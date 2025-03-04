import logging
from typing import Union

from spaceone.core import cache
from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token") or kwargs.get("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.identity_conn = SpaceConnector(service="identity",
                                            token=token)

    def get_user(self, domain_id: str, user_id: str) -> dict:
        system_token = config.get_global("TOKEN")
        response = self.identity_conn.dispatch(
            "User.list",
            {"user_id": user_id, "state": "ENABLED"},
            x_domain_id=domain_id,
            token=system_token,
        )
        users_info = response.get("results", [])
        if users_info:
            return users_info[0]
        else:
            return {}

    def get_domain_name(self, domain_id: str) -> str:
        system_token = config.get_global("TOKEN")

        domain_info = self.identity_conn.dispatch(
            "Domain.get", {"domain_id": domain_id}, token=system_token
        )
        return domain_info["name"]

    def list_domains(self, params: dict) -> dict:
        system_token = config.get_global("TOKEN")
        return self.identity_conn.dispatch("Domain.list", params, token=system_token)

    def list_enabled_domain_ids(self) -> list:
        system_token = config.get_global("TOKEN")
        params = {
            "query": {
                "filter": [
                    {"k": "state", "v": "ENABLED", "o": "eq"},
                ]
            }
        }
        response = self.identity_conn.dispatch(
            "Domain.list",
            params,
            token=system_token,
        )
        domains_info = response.get("results", [])
        domain_ids = [domain["domain_id"] for domain in domains_info]
        return domain_ids

    def check_workspace(self, workspace_id: str, domain_id: str) -> None:
        system_token = config.get_global("TOKEN")

        self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    @cache.cacheable(
        key="cost-analysis:workspace-name:{domain_id}:{workspace_id}:name", expire=300
    )
    def get_workspace(self, workspace_id: str, domain_id: str) -> str:
        try:
            workspace_info = self.identity_conn.dispatch(
                "Workspace.get",
                {"workspace_id": workspace_id},
                x_domain_id=domain_id,
            )
            return workspace_info["name"]
        except Exception as e:
            _LOGGER.error(f"[get_workspace] API Error: {e}")
            return workspace_id

    def list_workspaces(self, params: dict, domain_id: str, token: str = None) -> dict:
        if self.token_type == "SYSTEM_TOKEN" or token:
            return self.identity_conn.dispatch(
                "Workspace.list", params, x_domain_id=domain_id, token=token
            )
        else:
            return self.identity_conn.dispatch("Workspace.list", params)

    def list_workspace_users(self, params: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "WorkspaceUser.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("WorkspaceUser.list", params)

    def get_service_account_name_map(self, domain_id: str, workspace_id: str) -> dict:
        service_account_name_map = {}
        service_accounts = self.list_service_accounts(
            {
                "filter": [
                    {"k": "domain_id", "v": domain_id, "o": "eq"},
                    {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                ]
            },
            domain_id,
        )
        for service_account in service_accounts.get("results", []):
            service_account_name_map[service_account["service_account_id"]] = (
                service_account["name"]
            )
        return service_account_name_map

    def list_service_accounts(self, query: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "ServiceAccount.list", {"query": query}, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("ServiceAccount.list", {"query": query})

    @cache.cacheable(
        key="cost-analysis:project-name:{domain_id}:{workspace_id}:{project_id}",
        expire=300,
    )
    def get_project_name(self, project_id: str, workspace_id: str, domain_id: str):
        try:
            project_info = self.get_project(project_id, domain_id)
            return project_info["name"]
        except Exception as e:
            _LOGGER.error(f"[get_project_name] API Error: {e}")
            return project_id

    def get_project(self, project_id: str, domain_id: str):
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.get", {"project_id": project_id}, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch(
                "Project.get", {"project_id": project_id}
            )

    def get_project_name_map(self, domain_id: str, workspace_id: str) -> dict:
        project_name_map = {}
        params = {
            "query": {
                "filter": [
                    {"k": "domain_id", "v": domain_id, "o": "eq"},
                    {"k": "workspace_id", "v": workspace_id, "o": "eq"},
                ]
            }
        }

        response = self.list_projects(
            params=params,
            domain_id=domain_id,
        )
        for project in response.get("results", []):
            project_name_map[project["project_id"]] = project["name"]
        return project_name_map

    def list_projects(self, params: dict, domain_id: str):
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("Project.list", params)

    def list_project_groups(self, params: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "ProjectGroup.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("ProjectGroup.list", params)

    @cache.cacheable(key="cost-analysis:projects-in-pg:{project_group_id}", expire=300)
    def get_projects_in_project_group(self, project_group_id: str, domain_id: str):
        params = {
            "query": {
                "only": ["project_id"],
            },
            "project_group_id": project_group_id,
            "include_children": True,
        }

        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("Project.list", params)

    def list_role_bindings(self, params: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "RoleBinding.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("RoleBinding.list", params)

    def grant_token(
            self,
            params: dict,
    ) -> str:
        token_info = self.identity_conn.dispatch("Token.grant", params)
        return token_info["access_token"]
