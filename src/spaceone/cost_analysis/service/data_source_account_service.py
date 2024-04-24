import logging
from datetime import datetime
from typing import Union

from spaceone.core.service import *

from spaceone.cost_analysis.manager import DataSourceManager
from spaceone.cost_analysis.manager.data_source_account_manager import (
    DataSourceAccountManager,
)
from spaceone.cost_analysis.model.data_source_account.request import *
from spaceone.cost_analysis.model.data_source_account.response import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceAccountService(BaseService):
    resource = "DataSourceAccount"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr = DataSourceManager()
        self.data_source_account_mgr = DataSourceAccountManager()

    @transaction(
        permission="cost-analysis:DataSourceAccount.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def update(
        self, params: DataSourceAccountUpdateRequest
    ) -> Union[DataSourceAccountResponse, dict]:
        """Update data source account
        Args:
            params (dict): {
                'data_source_id': 'str',        # required
                'account_id': 'str',            # required
                'service_account_id': 'str',
                'project_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth
            }
        Returns:
            DataSourceAccountResponse
        """

        data_source_id = params.data_source_id
        account_id = params.account_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        # Check if the data source exists
        self.data_source_mgr.get_data_source(data_source_id, domain_id, workspace_id)

        data_source_account_vo = self.data_source_account_mgr.get_data_source_account(
            data_source_id, account_id, domain_id, workspace_id
        )
        data_source_account_vo = (
            self.data_source_account_mgr.update_data_source_account_by_vo(
                params.dict(exclude_unset=True), data_source_account_vo
            )
        )

        return DataSourceAccountResponse(**data_source_account_vo.to_dict())

    @transaction(
        permission="cost-analysis:DataSourceAccount.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    def reset(
        self, params: DataSourceAccountResetRequest
    ) -> Union[DataSourceAccountsResponse, dict]:
        """Reset data source account
        Args:
            params (dict): {
                'data_source_id': 'str',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth
            }
        Returns:
            dict
        """
        data_source_id = params.data_source_id
        workspace_id = params.workspace_id
        domain_id = params.domain_id

        # Check if the data source exists
        data_source_vo = self.data_source_mgr.get_data_source(
            data_source_id, domain_id, workspace_id
        )

        query = {
            "filter": [
                {"k": "data_source_id", "v": data_source_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }
        if data_source_vo.resource_group == "WORKSPACE":
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        (
            data_source_account_vos,
            total_count,
        ) = self.data_source_account_mgr.list_data_source_accounts(query)

        for data_source_account_vo in data_source_account_vos:
            update_params = {
                "project_id": None,
                "service_account_id": None,
                "is_sync": False,
                "updated_at": datetime.utcnow(),
            }
            if data_source_vo.resource_group == "DOMAIN":
                update_params["workspace_id"] = None
            self.data_source_account_mgr.update_data_source_account_by_vo(
                update_params, data_source_account_vo
            )

        data_source_accounts_info = [
            data_source_account_vo.to_dict()
            for data_source_account_vo in data_source_account_vos
        ]

        return DataSourceAccountsResponse(
            results=data_source_accounts_info, total_count=total_count
        )

    @transaction(
        permission="cost-analysis:DataSourceAccount.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get(
        self, params: DataSourceAccountGetRequest
    ) -> Union[DataSourceAccountResponse, dict]:
        """Get data source account
        Args:
            params (dict): {
                'account_id': 'str',        # required
                'data_source_id': 'str',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth
            }
        Returns:
            DataSourceAccountResponse
        """
        account_id = params.account_id
        data_source_id = params.data_source_id
        domain_id = params.domain_id
        workspace_id = params.workspace_id

        # Check if the data source exists
        self.data_source_mgr.get_data_source(data_source_id, domain_id, workspace_id)

        data_source_account_vo = self.data_source_account_mgr.get_data_source_account(
            data_source_id, account_id, domain_id, workspace_id
        )

        return DataSourceAccountResponse(**data_source_account_vo.to_dict())

    @transaction(
        permission="cost-analysis:DataSourceAccount.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "data_source_id",
            "account_id",
            "service_account_id",
            "project_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["name", "account_id", "data_source_id"])
    @convert_model
    def list(
        self, params: DataSourceAccountSearchQueryRequest
    ) -> Union[DataSourceAccountsResponse, dict]:
        """List cost reports
        Args:
            params (dict): {
                'query': 'dict',
                'account_id': 'str',
                'data_source_id': 'str',
                'service_account_id': 'str',
                'project_id': 'str',
                'workspace_id': 'str',
                'domain_id': 'str'
            }
        Returns:
            DataSourceAccountsResponse
        """

        query = params.query or {}

        (
            data_source_accounts,
            total_count,
        ) = self.data_source_account_mgr.list_data_source_accounts(query)

        data_source_accounts_info = [
            data_source_accounts_info.to_dict()
            for data_source_accounts_info in data_source_accounts
        ]

        return DataSourceAccountsResponse(
            results=data_source_accounts_info, total_count=total_count
        )

    @transaction(
        permission="cost-analysis:DataSourceAccount.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(["domain_id", "workspace_id"])
    @append_keyword_filter(["name", "account_id", "data_source_id"])
    @convert_model
    def stat(self, params: DataSourceAccountStatQueryRequest) -> dict:
        """Stat data source accounts
        Args:
            params (dict): {
                'query': 'dict',
                'workspace_id': 'str',     # injected from auth
                'domain_id': 'str'         # injected from auth
            }
        Returns:
            dict
        """

        query = params.query or {}
        return self.data_source_account_mgr.stat_data_source_accounts(query)
