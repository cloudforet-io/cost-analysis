from typing import Union

from spaceone.core.error import *
from spaceone.core.service import *
from spaceone.core.service.utils import *

from spaceone.cost_analysis.manager import (
    CostReportConfigManager,
    ReportAdjustmentPolicyManager,
    ReportAdjustmentManager,
)
from spaceone.cost_analysis.service.cost_report_serivce import CostReportService
from spaceone.cost_analysis.model.cost_report_config.request import *
from spaceone.cost_analysis.model.cost_report_config.response import *
from spaceone.cost_analysis.error.cost_report_config import (
    ERROR_COST_REPORT_CONFIG_NOT_ENABLED,
)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostReportConfigService(BaseService):
    resource = "CostReportConfig"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_config_mgr = CostReportConfigManager()
        self.adjustment_mgr = ReportAdjustmentManager()
        self.adjustment_policy_mgr = ReportAdjustmentPolicyManager()

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
                'scope': 'str',                # required
                'issue_day': 'int',
                'is_last_day': 'bool',
                'adjustment_options: 'dict',
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
                'adjustment_options: 'dict',
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

        if adjustment_options:= params.adjustment_options:
            current_adjustment_options = (
                    cost_report_config_vo.adjustment_options or {}
            )
            new_adjustment_options = current_adjustment_options.copy()

            new_adjustment_options["enabled"] = adjustment_options.get("enabled", False)

            if "period" in adjustment_options:
                new_adjustment_options["period"] = adjustment_options["period"]

            params.adjustment_options = new_adjustment_options

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

        adjustment_policy_vos = self.adjustment_policy_mgr.filter_policies(
            cost_report_config_id=cost_report_config_vo.cost_report_config_id,
            domain_id=params.domain_id,
        )

        for policy_vo in adjustment_policy_vos:
            adjustment_vos = self.adjustment_mgr.filter_adjustments(
                report_adjustment_policy_id=policy_vo.report_adjustment_policy_id,
                domain_id=params.domain_id,
            )
            for adj_vo in adjustment_vos:
                self.adjustment_mgr.delete_adjustment_by_vo(adj_vo)

            self.adjustment_policy_mgr.delete_policy_by_vo(policy_vo)

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

        if cost_report_config_vo.state != "ENABLED":
            raise ERROR_COST_REPORT_CONFIG_NOT_ENABLED(
                cost_report_config_id=cost_report_config_vo.cost_report_config_id,
                state=cost_report_config_vo.state,
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
    @append_query_filter(
        [
            "state",
            "scope",
            "domain_id",
            "cost_report_config_id",
        ]
    )
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
