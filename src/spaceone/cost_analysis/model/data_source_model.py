from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField(max_length=255)
    options = DictField(default={})
    metadata = DictField(default={})
    secret_id = StringField(max_length=40, default=None, null=True)
    schema = StringField(max_length=255, default=None, null=True)
    upgrade_mode = StringField(max_length=255, choices=('AUTO', 'MANUAL'), default='AUTO')

    def to_dict(self):
        return dict(self.to_mongo())


class DataSource(MongoModel):
    data_source_id = StringField(max_length=40, generate_id='ds', unique=True)
    name = StringField(max_length=255, unique_with='domain_id')
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    data_source_type = StringField(max_length=20, choices=('LOCAL', 'EXTERNAL'))
    provider = StringField(max_length=40, default=None, null=True)
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    template = DictField(default={})
    tags = DictField(default={})
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    last_synchronized_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'plugin_info',
            'template',
            'tags',
            'last_synchronized_at'
        ],
        'minimal_fields': [
            'data_source_id',
            'name',
            'state',
            'data_source_type',
            'provider'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            # 'data_source_id',
            'name',
            'state',
            'data_source_type',
            'provider',
            'domain_id'
        ]
    }
