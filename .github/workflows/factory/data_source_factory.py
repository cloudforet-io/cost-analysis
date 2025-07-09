import factory

from spaceone.core import utils
from spaceone.cost_analysis.model.data_source.database import DataSource, PluginInfo


class PluginInfoFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = PluginInfo

    plugin_id = 'plugin-aws-hyperbilling-cost-datasource'
    secret_id = utils.generate_id('secret')
    upgrade_mode = 'AUTO'
    version = '1.0'
    options = {}
    metadata = {
        'data_source_rules': [
            {
                'actions': {
                    'match_service_account': {
                        'source': 'account',
                        'target': 'data.account_id'
                    }
                },
                'conditions': [],
                'conditions_policy': 'ALWAYS',
                'name': 'match_service_account',
                'options': {
                    'stop_processing': True
                }
            }
        ]
    }


class DataSourceFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = DataSource

    plugin_info = factory.SubFactory(PluginInfoFactory)
    data_source_id = factory.LazyAttribute(lambda o: utils.generate_id('ds'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    state = 'ENABLED'
    data_source_type = 'EXTERNAL'
    provider = None
    template = {}
    tags = {'xxx': 'yyy'}
    cost_tag_keys = ['Name', 'Environment', 'Role']
    cost_additional_info_keys = ['raw_usage_type']
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
    last_synchronized_at = factory.Faker('date_time')
