from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DataSourceRuleCondition(EmbeddedDocument):
    key = StringField(required=True)
    value = StringField(required=True)
    operator = StringField(choices=('eq', 'contain', 'not', 'not_contain'))


class DataSourceRuleOptions(EmbeddedDocument):
    stop_processing = BooleanField(default=False)


class DataSourceRule(MongoModel):
    data_source_rule_id = StringField(max_length=40, generate_id='rule', unique=True)
    name = StringField(max_length=255, default='')
    order = IntField(required=True)
    conditions = ListField(EmbeddedDocumentField(DataSourceRuleCondition), default=[])
    conditions_policy = StringField(max_length=20, choices=('ALL', 'ANY', 'ALWAYS'))
    actions = DictField(default={})
    options = EmbeddedDocumentField(DataSourceRuleOptions, default=DataSourceRuleOptions)
    tags = DictField(default={})
    data_source = ReferenceField('DataSource', reverse_delete_rule=CASCADE)
    data_source_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'order',
            'conditions',
            'conditions_policy',
            'actions',
            'options',
            'tags'
        ],
        'minimal_fields': [
            'data_source_rule_id',
            'name',
            'order',
            'data_source_id'
        ],
        'ordering': ['order'],
        'indexes': [
            # 'data_source_rule_id',
            'order',
            'conditions_policy',
            'data_source_id',
            'domain_id'
        ]
    }
