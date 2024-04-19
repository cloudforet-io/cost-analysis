import logging
from typing import Generator, Union
from spaceone.core.service import BaseService, transaction
from spaceone.core.service.utils import convert_model
from spaceone.cost_analysis.plugin.data_source.model import (
    CostGetDataRequest,
    CostsResponse,
    CostGetLinkedAccountsRequest,
    AccountsResponse,
)

_LOGGER = logging.getLogger(__name__)


class CostService(BaseService):
    resource = "Cost"

    @transaction
    @convert_model
    def get_linked_accounts(
        self, params: CostGetLinkedAccountsRequest
    ) -> Union[AccountsResponse, dict]:
        """Get linked accounts
        Args:
            params (CostGetLinkedAccountsRequest): {
                'options': 'dict',      # Required
                'secret_data': 'dict',  # Required
                'schema': 'str',
                'domain_id': 'str'      # Required
            }
        Returns:
            AccountsResponse: {
                'results': 'list'
            }
        """

        func = self.get_plugin_method("get_linked_accounts")
        response = func(params.dict())
        return AccountsResponse(**response)

    @transaction
    @convert_model
    def get_data(
        self, params: CostGetDataRequest
    ) -> Generator[Union[CostsResponse, dict], None, None]:
        """Get external cost data

        Args:
            params (CostGetDataRequest): {
                'options': 'dict',          # Required
                'secret_data': 'dict',      # Required
                'schema': 'str',
                'task_options': 'dict',     # Required
                'domain_id': 'str'          # Required
            }

        Returns:
            Generator[CostsResponse, None, None]
            {
                'cost': 'float',
                'usage_quantity': 'float',
                'usage_unit': 'str',
                'provider': 'str',
                'region_code': 'str',
                'product': 'str',
                'usage_type': 'str',
                'resource': 'str',
                'tags': 'dict'
                'additional_info': 'dict'
                'data': 'dict'
                'billed_date': 'str'
            }
        """

        func = self.get_plugin_method("get_data")
        response_iterator = func(params.dict())
        for response in response_iterator:
            yield CostsResponse(**response)
