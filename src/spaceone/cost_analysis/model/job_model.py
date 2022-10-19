from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Error(EmbeddedDocument):
    error_code = StringField(max_length=128)
    message = StringField(max_length=2048)


class Changed(EmbeddedDocument):
    start = DateTimeField(required=True)
    end = DateTimeField(default=None, null=True)
    filter = DictField()


class Job(MongoModel):
    job_id = StringField(max_length=40, generate_id='job', unique=True)
    status = StringField(max_length=20, default='IN_PROGRESS', choices=('IN_PROGRESS', 'SUCCESS', 'FAILURE',
                                                                        'TIMEOUT', 'CANCELED'))
    options = DictField()
    error_code = StringField(max_length=254, default=None, null=True)
    error_message = StringField(default=None, null=True)
    total_tasks = IntField(default=0)
    remained_tasks = IntField(default=0)
    data_source_id = StringField(max_length=40, required=True)
    domain_id = StringField(max_length=40, required=True)
    changed = ListField(EmbeddedDocumentField(Changed), default=[])
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    finished_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'status',
            'error_code',
            'error_message',
            'total_tasks',
            'remained_tasks',
            'updated_at',
            'finished_at'
        ],
        'minimal_fields': [
            'job_id',
            'status',
            'total_tasks',
            'remained_tasks',
            'data_source_id'
        ],
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            # 'job_id',
            'status',
            'data_source_id',
            'domain_id',
            'created_at'
        ]
    }
