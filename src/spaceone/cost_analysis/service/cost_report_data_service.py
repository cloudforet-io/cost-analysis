import copy
import logging
import random
import re
import string
from typing import Union

from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.core import config

from spaceone.cost_analysis.manager.cost_report_data_manager import (
    CostReportDataManager,
)

from spaceone.cost_analysis.model.cost_report_data.request import *
from spaceone.cost_analysis.model.cost_report_data.response import *
from spaceone.cost_analysis.model.cost_report_data.database import CostReportData

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostReportDataService(BaseService):
    resource = "CostReportData"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_data_mgr = CostReportDataManager()

    @transaction(
        permission="cost-analysis:CostReportData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @append_query_filter(
        [
            "cost_report_config_id",
            "cost_report_data_id",
            "product",
            "provider",
            "is_confirmed",
            "data_source_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["product", "cost_report_data_id"])
    @convert_model
    def list(
        self, params: CostReportDataSearchQueryRequest
    ) -> Union[CostReportsDataResponse, dict]:
        """List cost report data"""

        query = params.query or {}
        (
            cost_report_data_vos,
            total_count,
        ) = self.cost_report_data_mgr.list_cost_reports_data(query)

        cost_reports_data_info = [
            cost_report_data_vo.to_dict()
            for cost_report_data_vo in cost_report_data_vos
        ]
        return CostReportsDataResponse(
            results=cost_reports_data_info, total_count=total_count
        )

    def analyze(self, params: CostReportDataAnalyzeQueryRequest) -> dict:
        """Analyze cost report data"""

        query = params.query or {}
        return self.cost_report_data_mgr.analyze_cost_reports_data(query)
