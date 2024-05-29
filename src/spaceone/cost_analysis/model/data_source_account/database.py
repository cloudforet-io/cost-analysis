from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DataSourceAccount(MongoModel):
    account_id = StringField(
        max_length=255, required=True, unique_with="data_source_id"
    )
    data_source_id = StringField(max_length=255, required=True)
    name = StringField(max_length=255)
    is_sync = BooleanField(default=False)
    is_linked = BooleanField(default=False)
    v_workspace_id = StringField(max_length=40, generate_id="v-workspace", unique=True)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "is_sync",
            "is_linked",
            "workspace_id",
            "updated_at",
        ],
        "minimal_fields": [
            "account_id",
            "data_source_id",
            "name",
            "is_sync",
            "is_linked",
            "v_workspace_id",
            "workspace_id",
        ],
        "ordering": ["-created_at"],
        "indexes": ["domain_id", "data_source_id", "account_id"],
    }
