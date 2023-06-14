import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *

__all__ = ['DataSourcePluginConnector']

_LOGGER = logging.getLogger(__name__)


class DataSourcePluginConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.secret_data = None
        self.options = None
        self.schema = None

    def initialize(self, endpoint):
        static_endpoint = self.config.get('endpoint')

        if static_endpoint:
            endpoint = static_endpoint

        e = parse_endpoint(endpoint)
        self.client = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version='plugin')

        self.secret_data = self.config.get('secret_data')
        self.options = self.config.get('options')
        self.schema = self.config.get('schema')

    def init(self, options, domain_id):
        response = self.client.DataSource.init({
            'options': options,
            'domain_id': domain_id
        })

        return self._change_message(response)

    def verify(self, options, secret_data, schema, domain_id):
        params = {
            'options': self.options or options,
            'secret_data': self.secret_data or secret_data,
            'schema': self.schema or schema,
            'domain_id': domain_id
        }

        self.client.DataSource.verify(params)

    def get_tasks(self, options, secret_data, schema, domain_id, start=None, last_synchronized_at=None):
        params = {
            'options': self.options or options,
            'secret_data': self.secret_data or secret_data,
            'schema': self.schema or schema,
            'start': start,
            'last_synchronized_at': last_synchronized_at,
            'domain_id': domain_id
        }

        response = self.client.Job.get_tasks(params)
        return self._change_message(response)

    def get_cost_data(self, options, secret_data, schema, task_options, domain_id):
        params = {
            'options': self.options or options,
            'secret_data': self.secret_data or secret_data,
            'schema': self.schema or schema,
            'task_options': task_options,
            'domain_id': domain_id
        }

        response_stream = self.client.Cost.get_data(params)
        return self._process_stream(response_stream)

    def _process_stream(self, response_stream):
        for message in response_stream:
            yield self._change_message(message)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
