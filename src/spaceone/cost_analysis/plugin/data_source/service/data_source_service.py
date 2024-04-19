import logging
from typing import Union
from spaceone.core.service import BaseService, transaction
from spaceone.core.service.utils import convert_model
from spaceone.cost_analysis.plugin.data_source.model import (
    DataSourceInitRequest,
    DataSourceVerifyRequest,
    PluginResponse,
)

_LOGGER = logging.getLogger(__name__)


class DataSourceService(BaseService):
    resource = "DataSource"

    @transaction
    @convert_model
    def init(self, params: DataSourceInitRequest) -> Union[PluginResponse, dict]:
        """init plugin by options

        Args:
            params (DataSourceInitRequest): {
                'options': 'dict',      # Required
                'domain_id': 'str'      # Required
            }

        Returns:
            PluginResponse: {
                'metadata': 'dict'
            }
        """

        func = self.get_plugin_method("init")
        response = func(params.dict())
        return PluginResponse(**response)

    @transaction
    @convert_model
    def verify(self, params: DataSourceVerifyRequest) -> None:
        """Verifying data source plugin

        Args:
            params (DataSourceVerifyRequest): {
                'options': 'dict',          # Required
                'secret_data': 'dict',      # Required
                'schema': 'str',
                'domain_id': 'str'          # Required
            }

        Returns:
            None
        """

        func = self.get_plugin_method("verify")
        func(params.dict())
