import calendar
import logging
import os
from typing import Union

from jinja2 import Environment, FileSystemLoader, select_autoescape

from spaceone.core import config, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.smtp_connector import SMTPConnector
from spaceone.cost_analysis.model import Budget

from spaceone.cost_analysis.model.cost_report.database import CostReport

_LOGGER = logging.getLogger(__name__)

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), f"../template")
JINJA_ENV = Environment(
    loader=FileSystemLoader(searchpath=TEMPLATE_PATH), autoescape=select_autoescape()
)

LANGUAGE_MAPPER = {
    "default": {
        "cost_report": "Your cost report is ready for review.",
        "budget_usage_alert": "{budget_name} Has Reached {threshold}% Utilization",
    },
    "ko": {
        "cost_report": "비용 리포트 전송",
        "budget_usage_alert": "{budget_name} 소진율 {threshold}% 초과 알림",
    },
    "en": {
        "cost_report": "Your cost report is ready for review.",
        "budget_usage_alert": "{budget_name} Has Reached {threshold}% Utilization",
    },
    "ja": {
        "cost_report": "費用レポートが確認のために準備されました。",
    },
}


class EmailManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.smtp_connector = SMTPConnector()

    def send_cost_report_email(
        self,
        user_id: str,
        email: str,
        cost_report_link: str,
        language: str,
        cost_report_vo: CostReport,
    ):
        service_name = self._get_service_name()
        language_map_info = LANGUAGE_MAPPER.get(language, "default")
        template = JINJA_ENV.get_template(f"cost_report_{language}.html")

        email_contents = template.render(
            user_name=user_id,
            report_number=cost_report_vo.report_number,
            name=cost_report_vo.name,
            report_date=cost_report_vo.issue_date,
            report_period=self.get_date_range_of_month(cost_report_vo.report_month),
            download_link=cost_report_link,
        )
        subject = f'[{service_name}] #{cost_report_vo.report_number} {language_map_info["cost_report"]}'

        self.smtp_connector.send_email(email, subject, email_contents)

    def send_budget_usage_alert_email(
        self,
        email: str,
        language: str,
        user_id: str,
        threshold: float,
        total_budget_usage: float,
        budget_percentage: float,
        today_date: str,
        workspace_name: str,
        console_link: str,
        budget_vo: Budget,
        target_name: Union[str, None] = None,
    ):
        service_name = self._get_service_name()
        language_map_info = LANGUAGE_MAPPER.get(language, "default")
        template = JINJA_ENV.get_template(f"budget_usage_alert_{language}.html")

        email_contents = template.render(
            user_name=user_id,
            workspace_name=workspace_name,
            budget_name=budget_vo.name,
            budget_target=target_name,
            budget_amount=budget_vo.limit,
            budget_cycle=budget_vo.time_unit,
            actual_cost=total_budget_usage,
            usage_rate=budget_percentage,
            today_date=today_date,
            budget_detail_link=console_link,
            currency=budget_vo.currency,
        )

        subject = f"[{service_name}] {language_map_info['budget_usage_alert'].format(budget_name=budget_vo.name, threshold=threshold)}"

        self.smtp_connector.send_email(email, subject, email_contents)

    @staticmethod
    def _get_service_name():
        return config.get_global("EMAIL_SERVICE_NAME", "Cloudforet")

    @staticmethod
    def get_date_range_of_month(report_month: str):
        year, month = report_month.split("-")
        _, last_day = calendar.monthrange(int(year), int(month))
        return f"{year}-{month}-01 ~ {year}-{month}-{last_day}"
