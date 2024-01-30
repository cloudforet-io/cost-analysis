import calendar
import logging
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from spaceone.core import config, utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.smtp_connector import SMTPConnector

from spaceone.cost_analysis.model.cost_report.database import CostReport

_LOGGER = logging.getLogger(__name__)

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), f"../template")
JINJA_ENV = Environment(
    loader=FileSystemLoader(searchpath=TEMPLATE_PATH), autoescape=select_autoescape()
)

LANGUAGE_MAPPER = {
    "default": {"cost_report": "Your cost report is ready for review."},
    "ko": {"cost_report": "비용 리포트 전송"},
    "en": {"cost_report": "Your cost report is ready for review."},
    "ja": {"cost_report": "費用レポートが確認のために準備されました。"},
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
            workspace_name=cost_report_vo.workspace_name,
            report_date=cost_report_vo.issue_date,
            report_period=self.get_date_range_of_month(cost_report_vo.report_month),
            download_link=cost_report_link,
        )
        subject = f'[{service_name}] #{cost_report_vo.report_number} {language_map_info["cost_report"]}'

        self.smtp_connector.send_email(email, subject, email_contents)

    @staticmethod
    def _get_service_name():
        return config.get_global("EMAIL_SERVICE_NAME", "Cloudforet")

    @staticmethod
    def get_date_range_of_month(report_month: str):
        year, month = report_month.split("-")
        _, last_day = calendar.monthrange(int(year), int(month))
        return f"{year}-{month}-01 ~ {year}-{month}-{last_day}"
