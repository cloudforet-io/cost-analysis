import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core import config
from spaceone.core.manager import BaseManager

from spaceone.cost_analysis.model.cost_report_config.database import CostReportConfig

_LOGGER = logging.getLogger(__name__)


class CostReportConfigManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_report_config_model = CostReportConfig

    def create_cost_report_config(self, params: dict) -> CostReportConfig:
        def _rollback(vo: CostReportConfig):
            _LOGGER.info(
                f"[create_cost_report._rollback] Delete cost_report: {vo.cost_report_config_id})"
            )
            vo.delete()

        cost_report_config_vo = self.cost_report_config_model.create(params)
        self.transaction.add_rollback(_rollback, cost_report_config_vo)

        return cost_report_config_vo

    def update_cost_report_config_by_vo(
        self, params: dict, cost_report_config_vo: CostReportConfig
    ) -> CostReportConfig:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_cost_report_config_by_vo._rollback] Revert Data: {old_data['cost_report_config_id']}"
            )
            cost_report_config_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cost_report_config_vo.to_dict())

        return cost_report_config_vo.update(params)

    def update_recipients_cost_report_config(
        self, params: dict, cost_report_config_vo: CostReportConfig
    ) -> CostReportConfig:
        def _rollback(old_data):
            _LOGGER.info(
                f'[update_recipients_cost_report_config._rollback] Revert Data: {old_data["cost_report_config_id"]}'
            )
            cost_report_config_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cost_report_config_vo.to_dict())

        return cost_report_config_vo.update(params)

    def enable_cost_report_config(
        self, cost_report_config_vo: CostReportConfig
    ) -> CostReportConfig:
        self.update_cost_report_config_by_vo(
            {"state": "ENABLED"}, cost_report_config_vo
        )

        return cost_report_config_vo

    def disable_cost_report_config(
        self, cost_report_config_vo: CostReportConfig
    ) -> CostReportConfig:
        self.update_cost_report_config_by_vo(
            {"state": "DISABLED"}, cost_report_config_vo
        )

        return cost_report_config_vo

    @staticmethod
    def delete_cost_report_config_by_vo(
        cost_report_config_vo: CostReportConfig,
    ) -> None:
        cost_report_config_vo.delete()

    def get_cost_report_config(
        self, domain_id: str, cost_report_config_id: str
    ) -> CostReportConfig:
        return self.cost_report_config_model.get(
            domain_id=domain_id, cost_report_config_id=cost_report_config_id
        )

    def list_cost_report_configs(
        self, query: dict, domain_id: str
    ) -> Tuple[QuerySet, int]:
        self.create_default_cost_report_config(domain_id)

        return self.cost_report_config_model.query(**query)

    def filter_cost_report_configs(self, **conditions) -> QuerySet:
        return self.cost_report_config_model.filter(**conditions)

    def stat_cost_report_configs(self, query: dict) -> dict:
        return self.cost_report_config_model.stat(**query)

    def create_default_cost_report_config(self, domain_id: str) -> None:
        cost_report_config_vos = self.cost_report_config_model.filter(
            state="ENABLED",
            domain_id=domain_id,
        )
        if not cost_report_config_vos:
            _LOGGER.debug(f"[create_default_cost_report_config] domain_id: {domain_id}")
            self.cost_report_config_model.create(
                {
                    "issue_day": config.get_global(
                        "COST_REPORT_CONFIG_DEFAULT_ISSUE_DAY", 10
                    ),
                    "currency": config.get_global(
                        "COST_REPORT_CONFIG_DEFAULT_CURRENCY", "KRW"
                    ),
                    "recipients": {},
                    "domain_id": domain_id,
                }
            )
