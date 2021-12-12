import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core import utils
from spaceone.cost_analysis.error import *

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='secret')

    def create_secret(self, domain_id, secret_data, schema):
        def _rollback(secret_id):
            _LOGGER.info(f'[create_secret._rollback] Delete secret : {secret_id}')
            self.delete_secret(secret_id, domain_id)

        params = {
            'name': utils.generate_id('secret-cost-data-source'),
            'data': secret_data,
            'secret_type': 'CREDENTIALS',
            'schema': schema,
            'domain_id': domain_id
        }

        response = self.secret_connector.dispatch('Secret.create', params)
        _LOGGER.debug(f'[_create_secret] {response}')
        secret_id = response['secret_id']

        self.transaction.add_rollback(_rollback, secret_id)

        return secret_id

    def delete_secret(self, secret_id, domain_id):
        params = {
            'secret_id': secret_id,
            'domain_id': domain_id
        }
        self.secret_connector.dispatch('Secret.delete', params)

    def list_secrets(self, query, domain_id):
        return self.secret_connector.dispatch('Secret.list', {'query': query, 'domain_id': domain_id})

    def get_secret(self, secret_id, domain_id):
        return self.secret_connector.dispatch('Secret.get', {'secret_id': secret_id, 'domain_id': domain_id})

    def get_secret_data(self, secret_id, domain_id):
        response = self.secret_connector.dispatch('Secret.get_data', {'secret_id': secret_id, 'domain_id': domain_id})
        return response['data']
