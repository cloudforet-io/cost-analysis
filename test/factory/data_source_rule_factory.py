import factory

from spaceone.core import utils
from spaceone.cost_analysis.model.data_source_rule_model import DataSourceRule


class DataSourceRuleFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = DataSourceRule

    order = 1
    conditions = []
    actions = {}
    data_source = None
    data_source_id = utils.generate_id('ds')

    data_source_rule_id = factory.LazyAttribute(lambda o: utils.generate_id('rule'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    conditions_policy = 'ALWAYS'
    rule_type = 'CUSTOM'
    options = {
        'stop_processing': True
    }
    tags = {
        'xxx': 'yy'
    }
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')




