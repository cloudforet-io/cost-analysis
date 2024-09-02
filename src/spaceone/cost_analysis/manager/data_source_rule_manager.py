import logging
import functools
from typing import Any

from mongoengine import QuerySet
from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.data_source_rule_model import (
    DataSourceRule,
    DataSourceRuleCondition,
)

_LOGGER = logging.getLogger(__name__)


class DataSourceRuleManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_rule_model: DataSourceRule = self.locator.get_model(
            "DataSourceRule"
        )

        self._workspace_info = {}
        self._project_info = {}
        self._service_account_info = {}
        self._data_source_rule_info = {}

    def create_data_source_rule(self, params):
        def _rollback(data_source_rule_vo: DataSourceRule):
            _LOGGER.info(
                f"[create_data_source_rule._rollback] "
                f"Delete event rule : {data_source_rule_vo.name} "
                f"({data_source_rule_vo.data_source_rule_id})"
            )
            data_source_rule_vo.delete()

        data_source_rule_vo: DataSourceRule = self.data_source_rule_model.create(params)
        self.transaction.add_rollback(_rollback, data_source_rule_vo)

        return data_source_rule_vo

    def update_data_source_rule(self, params):
        data_source_rule_vo: DataSourceRule = self.get_data_source_rule(
            params["data_source_rule_id"], params["domain_id"]
        )
        return self.update_data_source_rule_by_vo(params, data_source_rule_vo)

    def update_data_source_rule_by_vo(self, params, data_source_rule_vo):
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_data_source_rule_by_vo._rollback] Revert Data : "
                f'{old_data["data_source_rule_id"]}'
            )
            data_source_rule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_source_rule_vo.to_dict())

        return data_source_rule_vo.update(params)

    def delete_data_source_rule(self, data_source_rule_id, domain_id):
        data_source_rule_vo: DataSourceRule = self.get_data_source_rule(
            data_source_rule_id, domain_id
        )
        self.delete_data_source_rule_by_vo(data_source_rule_vo)

    @staticmethod
    def delete_data_source_rule_by_vo(data_source_rule_vo):
        data_source_rule_vo.delete()

    def get_data_source_rule(
        self, data_source_rule_id: str, domain_id: str, workspace_id: str = None
    ):
        conditions = {
            "data_source_rule_id": data_source_rule_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        return self.data_source_rule_model.get(**conditions)

    def filter_data_source_rules(self, **conditions):
        return self.data_source_rule_model.filter(**conditions)

    def list_data_source_rules(self, query: dict):
        return self.data_source_rule_model.query(**query)

    def stat_data_source_rules(self, query):
        return self.data_source_rule_model.stat(**query)

    def change_cost_data(self, cost_data: dict, workspace_id: str = None) -> dict:
        data_source_id = cost_data["data_source_id"]
        domain_id = cost_data["domain_id"]
        (
            managed_data_source_rule_vos,
            custom_data_source_rule_vos,
        ) = self._get_data_source_rules(data_source_id, domain_id)

        cost_data = self._apply_data_source_rule_to_cost_data(
            cost_data, managed_data_source_rule_vos, domain_id, workspace_id
        )

        cost_data = self._apply_data_source_rule_to_cost_data(
            cost_data, custom_data_source_rule_vos, domain_id, workspace_id
        )

        return cost_data

    def _apply_data_source_rule_to_cost_data(
        self,
        cost_data: dict,
        data_source_rule_vos: QuerySet,
        domain_id: str,
        workspace_id: str = None,
    ):
        for data_source_rule_vo in data_source_rule_vos:
            is_match = self._change_cost_data_by_rule(cost_data, data_source_rule_vo)
            if is_match:
                cost_data = self._change_cost_data_with_actions(
                    cost_data,
                    data_source_rule_vo.actions,
                    domain_id,
                    workspace_id,
                )

            if is_match and data_source_rule_vo.options.stop_processing:
                break

        return cost_data

    def _change_cost_data_with_actions(
        self,
        cost_data: dict,
        actions: dict,
        domain_id: str,
        workspace_id: str = None,
    ):
        for action, value in actions.items():

            if action == "match_workspace" and value:
                source = value["source"]
                target_key = value.get("target", "workspace_id")
                target_value = utils.get_dict_value(cost_data, source)

                if target_value:
                    workspace_info = self._get_workspace(
                        target_key, target_value, domain_id
                    )
                    if workspace_info:
                        cost_data["workspace_id"] = workspace_info["workspace_id"]

            elif action == "change_project" and value:
                cost_data["project_id"] = value

            elif action == "match_project" and value:
                source = value["source"]
                target_key = value.get("target", "project_id")
                target_value = utils.get_dict_value(cost_data, source)
                if target_value:
                    project_info = self._get_project(
                        target_key, target_value, domain_id, workspace_id
                    )
                    if project_info:
                        cost_data["project_id"] = project_info["project_id"]
                        if cost_data.get("workspace_id") is None:
                            cost_data["workspace_id"] = project_info["workspace_id"]

            elif action == "match_service_account" and value:
                source = value["source"]
                target_key = value.get("target", "service_account_id")
                target_value = utils.get_dict_value(cost_data, source)
                all_workspaces = value.get("all_workspaces", False)

                if all_workspaces:
                    workspace_id = None

                if target_value:
                    service_account_info = self._get_service_account(
                        target_key, target_value, domain_id, workspace_id
                    )

                    if service_account_info:
                        cost_data["service_account_id"] = service_account_info[
                            "service_account_id"
                        ]
                        cost_data["project_id"] = service_account_info.get("project_id")
                        if cost_data.get("workspace_id") is None:
                            cost_data["workspace_id"] = service_account_info.get(
                                "workspace_id"
                            )
                        elif all_workspaces:
                            cost_data["workspace_id"] = service_account_info.get(
                                "workspace_id"
                            )

            if action == "add_additional_info" and value:
                cost_data["additional_info"] = cost_data.get("additional_info", {})
                cost_data["additional_info"].update(value)

        return cost_data

    def _get_service_account(
        self, target_key, target_value, domain_id: str, workspace_id: str = None
    ):
        if (
            f"service-account:{domain_id}:{target_key}:{target_value}:{workspace_id}"
            in self._service_account_info
        ):
            return self._service_account_info[
                f"service-account:{domain_id}:{target_key}:{target_value}:{workspace_id}"
            ]

        query = {
            "filter": [
                {"k": target_key, "v": target_value, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ],
            "only": ["service_account_id", "project_id", "workspace_id", "tags"],
        }

        if workspace_id:
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_service_accounts(query, domain_id)
        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        service_account_info = None
        if total_count > 0:
            service_account_info = results[0]

        self._service_account_info[
            f"service-account:{domain_id}:{target_key}:{target_value}:{workspace_id}"
        ] = service_account_info
        return service_account_info

    def _get_project(
        self, target_key, target_value, domain_id: str, workspace_id: str = None
    ):
        if f"project:{domain_id}:{target_key}:{target_value}" in self._project_info:
            return self._project_info[
                f"project:{domain_id}:{target_key}:{target_value}"
            ]

        query = {
            "filter": [{"k": target_key, "v": target_value, "o": "eq"}],
            "only": ["project_id"],
        }
        if workspace_id:
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_projects({"query": query}, domain_id)
        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        project_info = None
        if total_count > 0:
            project_info = results[0]

        self._project_info[f"project:{domain_id}:{target_key}:{target_value}"] = (
            project_info
        )
        return project_info

    def _get_workspace(
        self, target_key: str, target_value: Any, domain_id: str
    ) -> dict:
        if f"workspace:{domain_id}:{target_key}:{target_value}" in self._workspace_info:
            return self._workspace_info[
                f"workspace:{domain_id}:{target_key}:{target_value}"
            ]

        query = {
            "filter": [{"k": target_key, "v": target_value, "o": "eq"}],
            "only": ["workspace_id"],
        }

        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_projects({"query": query}, domain_id)
        results = response.get("results", [])
        total_count = response.get("total_count", 0)

        workspace_info = None
        if total_count > 0:
            workspace_info = results[0]

        self._workspace_info[f"workspace:{domain_id}:{target_key}:{target_value}"] = (
            workspace_info
        )
        return workspace_info

    def _change_cost_data_by_rule(
        self, cost_data: dict, data_source_rule_vo: DataSourceRule
    ) -> bool:
        conditions_policy = data_source_rule_vo.conditions_policy

        if conditions_policy == "ALWAYS":
            return True
        else:
            results = list(
                map(
                    functools.partial(self._check_condition, cost_data),
                    data_source_rule_vo.conditions,
                )
            )

            if conditions_policy == "ALL":
                return all(results)
            else:
                return any(results)

    @staticmethod
    def _check_condition(cost_data, condition: DataSourceRuleCondition):
        cost_value = utils.get_dict_value(cost_data, condition.key)
        condition_value = condition.value
        operator = condition.operator

        if cost_value is None:
            return False

        if operator == "eq":
            if cost_value == condition_value:
                return True
            else:
                return False
        elif operator == "contain":
            if cost_value.lower().find(condition_value.lower()) >= 0:
                return True
            else:
                return False
        elif operator == "not":
            if cost_value != condition_value:
                return True
            else:
                return False
        elif operator == "not_contain":
            if cost_value.lower().find(condition_value.lower()) < 0:
                return True
            else:
                return False

        return False

    def _get_data_source_rules(self, data_source_id, domain_id):
        if data_source_id in self._data_source_rule_info:
            return self._data_source_rule_info[data_source_id].get(
                "managed", []
            ), self._data_source_rule_info[data_source_id].get("custom", [])

        managed_query = self._make_data_source_rule_query(
            data_source_id, "MANAGED", domain_id
        )
        managed_data_source_rule_vos, total_count = self.list_data_source_rules(
            managed_query
        )

        custom_query = self._make_data_source_rule_query(
            data_source_id, "CUSTOM", domain_id
        )
        custom_data_source_rule_vos, total_count = self.list_data_source_rules(
            custom_query
        )

        self._data_source_rule_info[data_source_id] = {}
        self._data_source_rule_info[data_source_id][
            "managed"
        ] = managed_data_source_rule_vos
        self._data_source_rule_info[data_source_id][
            "custom"
        ] = custom_data_source_rule_vos

        return managed_data_source_rule_vos, custom_data_source_rule_vos

    @staticmethod
    def _make_data_source_rule_query(data_source_id, rule_type, domain_id):
        return {
            "filter": [
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "rule_type", "v": rule_type, "o": "eq"},
            ],
            "sort": [{"key": "order"}],
        }
