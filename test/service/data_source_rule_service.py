import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.model.mongo_model import MongoModel
from spaceone.core.transaction import Transaction

from spaceone.cost_analysis.service.data_source_rule_service import DataSourceRuleService
from spaceone.cost_analysis.model.data_source_rule_model import DataSourceRule
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager

from spaceone.cost_analysis.info.data_source_rule_info import *
from test.factory.data_source_factory import DataSourceFactory
from test.factory.data_source_rule_factory import DataSourceRuleFactory


class TestDataSourceRuleService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.cost_analysis')
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'cost_analysis',
            'api_class': 'DataSourceRule'
        })
        cls.data_source_vo = DataSourceFactory(domain_id=cls.domain_id)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete data source rule')
        self.data_source_vo.delete()

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_create_data_source_rule(self, *args):
        params = {
            'data_source_id': self.data_source_vo.data_source_id,
            'name': 'Test Data Source Rule',
            'conditions_policy': 'ALWAYS',
            'actions': {
                'match_service_account': {
                    'source': 'account',
                    'target': 'data.account_id'
                }
            },
            'options': {},
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        self.transaction.method = 'create'
        data_source_rule_svc = DataSourceRuleService(transaction=self.transaction)
        data_source_rule_vo = data_source_rule_svc.create(params.copy())
        print(DataSourceRuleInfo(data_source_rule_vo))
        print_data(data_source_rule_vo.to_dict(), 'test_create_data_source_rule')

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_update_data_source_rule(self, *args):
        data_source_rule_vo = DataSourceRuleFactory(domain_id=self.domain_id,
                                                    data_source_id=self.data_source_vo.data_source_id,
                                                    data_source=self.data_source_vo)

        name = 'Update name'
        params = {
            'name': name,
            'data_source_rule_id': data_source_rule_vo.data_source_rule_id,
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        data_source_rule_svc = DataSourceRuleService(transaction=self.transaction)
        update_data_source_rule_vo = data_source_rule_svc.update(params.copy())

        print_data(update_data_source_rule_vo.to_dict(), 'test_update_data_source_rule')
        print(DataSourceRuleInfo(update_data_source_rule_vo))

        self.assertEqual(name, update_data_source_rule_vo.name)
        self.assertEqual(params['tags'], update_data_source_rule_vo.tags)
        self.assertEqual(params['domain_id'], update_data_source_rule_vo.domain_id)

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_delete_data_source_rule(self, *args):
        data_source_rule_vo = DataSourceRuleFactory(domain_id=self.domain_id)
        params = {
            'data_source_rule_id': data_source_rule_vo.data_source_rule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'delete'
        data_source_rule_svc = DataSourceRuleService(transaction=self.transaction)
        result = data_source_rule_svc.delete(params)

        self.assertIsNone(result)

    @patch.object(IdentityManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_get_data_source_rule(self, *args):
        data_source_rule_vo = DataSourceRuleFactory(domain_id=self.domain_id)
        params = {
            'data_source_rule_id': data_source_rule_vo.data_source_rule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        data_source_rule_svc = DataSourceRuleService(transaction=self.transaction)
        get_data_source_rule_vo = data_source_rule_svc.get(params)

        print_data(get_data_source_rule_vo.to_dict(), 'test_get_data_source_rule')
        DataSourceRuleInfo(get_data_source_rule_vo)

        self.assertIsInstance(get_data_source_rule_vo, DataSourceRule)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
