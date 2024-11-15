import logging
from datetime import datetime
from typing import Tuple, Union

from dateutil.relativedelta import relativedelta
from mongoengine import QuerySet

from spaceone.core import queue, utils, config
from spaceone.core.error import *
from spaceone.core.manager import BaseManager

from spaceone.cost_analysis.error import ERROR_INVALID_DATE_RANGE
from spaceone.cost_analysis.model.unified_cost.database import UnifiedCost

_LOGGER = logging.getLogger(__name__)


class UnifiedCostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unified_cost_model = UnifiedCost

    def create_unified_cost(self, params: dict) -> UnifiedCost:
        def _rollback(vo: UnifiedCost):
            _LOGGER.info(
                f"[create_unified_cost._rollback] Delete unified_cost : {vo.unified_cost_id}, {vo.to_dict()} "
            )
            vo.delete()

        unified_cost_vo: UnifiedCost = self.unified_cost_model.create(params)
        self.transaction.add_rollback(_rollback, unified_cost_vo)

        return unified_cost_vo

    @staticmethod
    def delete_unified_cost_by_vo(unified_cost_vo: UnifiedCost) -> None:
        unified_cost_vo.delete()

    def get_unified_cost(
        self,
        unified_cost_id: str,
        domain_id: str,
        workspace_id: str = None,
        project_id: Union[list, str] = None,
    ) -> UnifiedCost:
        conditions = {
            "unified_cost_id": unified_cost_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if project_id:
            conditions["project_id"] = project_id

        return self.unified_cost_model.get(**conditions)

    def filter_unified_costs(self, **conditions) -> QuerySet:
        return self.unified_cost_model.filter(**conditions)

    def list_unified_costs(self, query: dict) -> Tuple[QuerySet, int]:
        return self.unified_cost_model.query(**query)

    def analyze_unified_costs(self, query: dict, target="SECONDARY_PREFERRED") -> dict:
        query["target"] = target
        query["date_field"] = "billed_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_unified_costs] query: {query}")

        return self.unified_cost_model.analyze(**query)

    def analyze_yearly_unified_costs(self, query, target="SECONDARY_PREFERRED"):
        query["target"] = target
        query["date_field"] = "billed_year"
        query["date_field_format"] = "%Y"
        _LOGGER.debug(f"[analyze_yearly_unified_costs] query: {query}")

        return self.unified_cost_model.analyze(**query)

    def analyze_unified_costs_by_granularity(self, query: dict) -> dict:
        granularity = query["granularity"]
        self._check_unified_cost_data_range(query)

        if granularity == "DAILY":
            raise ERROR_INVALID_PARAMETER(
                key="query.granularity", reason=f"{granularity} is not supported"
            )
        elif granularity == "MONTHLY":
            response = self.analyze_unified_costs(query)
        else:
            response = self.analyze_yearly_unified_costs(query)

        return response

    def stat_unified_costs(self, query) -> dict:
        return self.unified_cost_model.stat(**query)

    @staticmethod
    def push_unified_cost_job_task(params: dict) -> None:
        token = config.get_global("TOKEN")
        task = {
            "name": "run_unified_cost",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                {
                    "locator": "SERVICE",
                    "name": "UnifiedCostService",
                    "metadata": {"token": token},
                    "method": "run_unified_cost",
                    "params": {"params": params},
                }
            ],
        }

        _LOGGER.debug(f"[push_job_task] task param: {params}")

        queue.put("cost_analysis_q", utils.dump_json(task))

    @staticmethod
    def get_exchange_currency(cost: float, currency: str, currency_map: dict) -> dict:
        cost_info = {}
        for convert_currency in currency_map.keys():
            cost_info.update(
                {
                    convert_currency: currency_map[currency][
                        f"{currency}/{convert_currency}"
                    ]
                    * cost
                }
            )

        return cost_info

    def _check_unified_cost_data_range(self, query: dict):
        start_str = query.get("start")
        end_str = query.get("end")
        granularity = query.get("granularity")

        start = self._parse_start_time(start_str, granularity)
        end = self._parse_end_time(end_str, granularity)
        now = datetime.utcnow().date()

        if len(start_str) != len(end_str):
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="Start date and end date must be the same format.",
            )

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="End date must be greater than start date.",
            )

        if granularity == "MONTHLY":
            if start + relativedelta(months=24) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 12 months.",
                )

            if start + relativedelta(months=48) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For MONTHLY, you cannot request data older than 3 years.",
                )
        elif granularity == "YEARLY":
            if start + relativedelta(years=5) < now.replace(month=1, day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For YEARLY, you cannot request data older than 3 years.",
                )

    @staticmethod
    def _convert_date_from_string(date_str, key, granularity):
        if granularity == "YEARLY":
            date_format = "%Y"
            date_type = "YYYY"
        else:
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            else:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_type)

    def _parse_start_time(self, date_str, granularity):
        return self._convert_date_from_string(date_str.strip(), "start", granularity)

    def _parse_end_time(self, date_str, granularity):
        end = self._convert_date_from_string(date_str.strip(), "end", granularity)

        if granularity == "YEARLY":
            return end + relativedelta(years=1)
        else:
            return end + relativedelta(months=1)
