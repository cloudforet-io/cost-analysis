from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024, default=None, null=True)
    interval = IntField(min_value=1, max_value=60, default=None, null=True)
    hours = ListField(IntField(), default=None, null=True)

    def to_dict(self):
        return dict(self.to_mongo())


class Schedule(MongoModel):
    schedule_id = StringField(max_length=40, generate_id='sch', unique=True)
    name = StringField(max_length=40)
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    schedule = EmbeddedDocumentField(Scheduled, default=Scheduled)
    options = DictField(required=True)
    tags = DictField(default={})
    data_source = ReferenceField('DataSource', reverse_delete_rule=CASCADE)
    data_source_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    last_scheduled_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'schedule',
            'options',
            'tags',
            'last_scheduled_at'
        ],
        'minimal_fields': [
            'schedule_id',
            'name',
            'state'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            # 'schedule_id',
            'name',
            'state',
            'data_source_id',
            'domain_id'
        ]
    }
