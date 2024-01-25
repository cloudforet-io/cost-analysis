from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["CostReportDataResponse", "CostReportsDataResponse"]


class CostReportDataResponse(BaseModel):
    cost_report_data_id: Union[str, None] = None
    cost: Union[dict, None] = None
    cost_report_name: Union[str, None] = None
    report_year: Union[str, None] = None
    report_month: Union[str, None] = None
    is_confirmed: Union[bool, None] = None
    provider: Union[str, None] = None
    product: Union[str, None] = None
    service_account_name: Union[str, None] = None
    data_source_name: Union[str, None] = None
    project_name: Union[str, None] = None
    workspace_name: Union[str, None] = None
    service_account_id: Union[str, None] = None
    data_source_id: Union[str, None] = None
    cost_report_id: Union[str, None] = None
    cost_report_config_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None


class CostReportsDataResponse(BaseModel):
    results: List[CostReportDataResponse]
    total_count: int
