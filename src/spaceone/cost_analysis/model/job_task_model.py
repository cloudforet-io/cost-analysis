from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class JobTask(MongoModel):
    job_task_id = StringField(max_length=40, generate_id="job-task", unique=True)
    status = StringField(
        max_length=20,
        default="PENDING",
        choices=("PENDING", "IN_PROGRESS", "SUCCESS", "FAILURE", "CANCELED"),
    )
    options = DictField()
    created_count = IntField(default=0)
    error_code = StringField(max_length=254, default=None, null=True)
    error_message = StringField(default=None, null=True)
    resource_group = StringField(max_length=40, choices=["DOMAIN", "WORKSPACE"])
    job_id = StringField(max_length=40, required=True)
    data_source_id = StringField(max_length=40, required=True)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40, required=True)
    created_at = DateTimeField(auto_now_add=True)
    started_at = DateTimeField(default=None, null=True)
    updated_at = DateTimeField(auto_now=True)
    finished_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "status",
            "created_count",
            "error_code",
            "error_message",
            "started_at",
            "updated_at",
            "finished_at",
        ],
        "minimal_fields": [
            "job_task_id",
            "status",
            "created_count",
            "job_id",
            "data_source_id",
            "workspace_id",
            "created_at",
        ],
        "ordering": ["-created_at"],
        "indexes": [
            # 'job_task_id',
            "status",
            "job_id",
            "resource_group",
            "data_source_id",
            "workspace_id",
            "domain_id",
            "created_at",
        ],
    }
