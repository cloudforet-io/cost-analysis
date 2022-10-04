import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *

__all__ = ['DataSourcePluginConnector']

_LOGGER = logging.getLogger(__name__)


class DataSourcePluginConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self.client = None

    def initialize(self, endpoint):
        static_endpoint = self.config.get('endpoint')

        if static_endpoint:
            endpoint = static_endpoint

        e = parse_endpoint(endpoint)
        self.client = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version='plugin')

    def init(self, options, domain_id):
        response = self.client.DataSource.init({
            'options': options,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def verify(self, options, secret_data, schema, domain_id):
        params = {
            'options': options,
            'secret_data': secret_data,
            'schema': schema,
            'domain_id': domain_id
        }

        self.client.DataSource.verify(params, metadata=self.transaction.get_connection_meta())

    def get_tasks(self, options, secret_data, schema, domain_id, start=None, last_synchronized_at=None):
        params = {
            'options': options,
            'secret_data': secret_data,
            'schema': schema,
            'start': start,
            'last_synchronized_at': last_synchronized_at,
            'domain_id': domain_id
        }

        response = self.client.Job.get_tasks(params, metadata=self.transaction.get_connection_meta())
        return self._change_message(response)

    def get_cost_data(self, options, secret_data, schema, task_options, domain_id):
        params = {
            'options': options,
            'secret_data': secret_data,
            'schema': schema,
            'task_options': task_options,
            'domain_id': domain_id
        }

        response_stream = self.client.Cost.get_data(params, metadata=self.transaction.get_connection_meta())
        return self._process_stream(response_stream)

    def _process_stream(self, response_stream):
        for message in response_stream:
            yield self._change_message(message)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
