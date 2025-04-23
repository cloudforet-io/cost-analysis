from typing import Union

from spaceone.core.error import *
from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager.cost_report_config_manager import (
    CostReportConfigManager,
)
from spaceone.cost_analysis.manager.currency_manager import CurrencyManager
from spaceone.cost_analysis.service.cost_report_serivce import CostReportService
from spaceone.cost_analysis.model.cost_report_config.request import *
from spaceone.cost_analysis.model.cost_report_config.response import *


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostReportConfigService(BaseService):
    resource = "CostReportConfig"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_config_mgr = CostReportConfigManager()

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def create(
        self, params: CostReportConfigCreateRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Create cost report config

        Args:
            params (CostReportConfigCreateRequest): {
                'issue_day': 'int',
                'is_last_day': 'bool',
                'currency': 'str',             # required
                'recipients': 'dict',
                'data_source_filter': 'dict'
                'domain_id': 'str'             # injected from auth (required)
                }

        Returns:
            CostReportConfigResponse:
        """
        if not params.is_last_day:
            if not params.issue_day:
                raise ERROR_REQUIRED_PARAMETER(key="issue_day")

        cost_report_vo = self.cost_report_config_mgr.create_cost_report_config(
            params.dict()
        )

        return CostReportConfigResponse(**cost_report_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def update(
        self, params: CostReportConfigUpdateRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Update cost report config

        Args:
            params (CostReportConfigUpdateRequest): {
                'cost_report_config_id': 'str',     # required
                'issue_day': 'int',
                'is_last_day': 'bool',
                'currency': 'str',
                'recipients': 'dict',
                'data_source_filter': 'dict'
                'domain_id': 'str'                  # injected from auth (required)
            }
        Returns:
            CostReportConfigResponse:
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id,
            params.cost_report_config_id,
        )

        if params.is_last_day is None:
            params.is_last_day = False

        cost_report_config_vo = (
            self.cost_report_config_mgr.update_cost_report_config_by_vo(
                params.dict(exclude_unset=True), cost_report_config_vo
            )
        )

        return CostReportConfigResponse(**cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def update_recipients(
        self, params: CostReportConfigUpdateRecipientsRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Update recipients cost report config

        Args:
            params (CostReportConfigUpdateRequest): {
                'cost_report_config_id': 'str',     # required
                'recipients': 'dict',
                'domain_id': 'str'                  # injected from auth (required)

        Returns:
            CostReportConfigResponse:
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id,
            params.cost_report_config_id,
        )

        cost_report_config_vo = (
            self.cost_report_config_mgr.update_recipients_cost_report_config(
                params.dict(exclude_unset=True), cost_report_config_vo
            )
        )

        return CostReportConfigResponse(**cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def enable(
        self, params: CostReportConfigEnableRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Enable cost report config

        Args:
            params (CostReportConfigEnableRequest): {
                'cost_report_config_id': 'str',     # required
                'domain_id': 'str'                  # injected from auth (required)

        Returns:
            CostReportConfigResponse:
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id, params.cost_report_config_id
        )

        cost_report_config_vo = self.cost_report_config_mgr.enable_cost_report_config(
            cost_report_config_vo
        )

        return CostReportConfigResponse(**cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def disable(
        self, params: CostReportConfigDisableRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Disable cost report config

        Args:
            params (CostReportConfigDisableRequest): {
                'cost_report_config_id': 'str',     # required
                'domain_id': 'str'                  # injected from auth (required)

        Returns:
            CostReportConfigResponse:
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id,
            params.cost_report_config_id,
        )

        cost_report_config_vo = self.cost_report_config_mgr.disable_cost_report_config(
            cost_report_config_vo
        )

        return CostReportConfigResponse(**cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def delete(self, params: CostReportConfigDeleteRequest) -> None:
        """Delete cost report config

        Args:
            params (CostReportConfigDeleteRequest): {
                'cost_report_config_id': 'str',     # required
                'domain_id': 'str'                  # injected from auth (required)

        Returns:
            None
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id, params.cost_report_config_id
        )

        self.cost_report_config_mgr.delete_cost_report_config_by_vo(
            cost_report_config_vo
        )

    @transaction(
        permission="cost-analysis:CostReportConfig.write", role_types=["DOMAIN_ADMIN"]
    )
    @convert_model
    def run(self, params: CostReportConfigRunRequest) -> None:
        """RUN cost report config
        Args:
            params (CostReportConfigRunRequest): {
                'cost_report_config_id': 'str',     # required
                'domain_id': 'str'                  # injected from auth (required)
            }
        Returns:
            None
        """

        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id, params.cost_report_config_id
        )

        cost_report_service = CostReportService()
        cost_report_service.create_cost_report(cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def get(
        self, params: CostReportConfigGetRequest
    ) -> Union[CostReportConfigResponse, dict]:
        """Get cost report config

        Args:
            params (CostReportConfigGetRequest): {
                'cost_report_config_id': 'str',     # required
                'domain_id': 'str'                  # injected from auth (required)

        Returns:
            CostReportConfigResponse:
        """
        cost_report_config_vo = self.cost_report_config_mgr.get_cost_report_config(
            params.domain_id, params.cost_report_config_id
        )

        return CostReportConfigResponse(**cost_report_config_vo.to_dict())

    @transaction(
        permission="cost-analysis:CostReportConfig.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(["state", "domain_id", "cost_report_config_id"])
    @convert_model
    def list(
        self, params: CostReportConfigSearchQueryRequest
    ) -> Union[CostReportConfigsResponse, dict]:
        """List cost report configs

        Args:
            params (CostReportConfigSearchQueryRequest): {
                'query': 'dict',
                'cost_report_config_id': 'str',
                'state": 'str',
                'workspace_id': 'str'
                'domain_id': 'str'                # injected from auth (required)
            }

        Returns:
            CostReportConfigsResponse:
        """

        query = params.query or {}
        (
            cost_report_config_vos,
            total_count,
        ) = self.cost_report_config_mgr.list_cost_report_configs(
            query, params.domain_id
        )

        cost_report_configs_info = [
            cost_report_config_vo.to_dict()
            for cost_report_config_vo in cost_report_config_vos
        ]

        return CostReportConfigsResponse(
            results=cost_report_configs_info, total_count=total_count
        )

    @transaction(
        permission="cost-analysis:CostReportConfig.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(["cost_report_config_id", "domain_id"])
    @convert_model
    def stat(self, params: CostReportConfigStatQueryRequest) -> dict:
        """Stat cost report configs

        Args:
            params (CostReportConfigStatRequest): {
                'query': 'dict',
                'cost_report_config_id': 'str',
                'domain_id': 'str'                # injected from auth (required)
                'workspace_id': 'str'
            }

        Returns:
            CostReportConfigStatResponse:
        """
        query = params.query or {}
        return self.cost_report_config_mgr.stat_cost_report_configs(query)
