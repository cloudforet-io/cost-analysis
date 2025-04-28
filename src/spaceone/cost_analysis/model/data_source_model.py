from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField(max_length=255)
    options = DictField(default={})
    metadata = DictField(default={})
    secret_id = StringField(max_length=40, default=None, null=True)
    schema_id = StringField(max_length=255, default=None, null=True)
    upgrade_mode = StringField(
        max_length=255, choices=("AUTO", "MANUAL"), default="AUTO"
    )

    def to_dict(self):
        return dict(self.to_mongo())


class SecretFilter(EmbeddedDocument):
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED")
    )
    secrets = ListField(StringField(max_length=40), defualt=None, null=True)
    service_accounts = ListField(StringField(max_length=40), default=None, null=True)
    schemas = ListField(StringField(max_length=40), default=None, null=True)

    def to_dict(self):
        return dict(self.to_mongo())


class DataSource(MongoModel):
    data_source_id = StringField(max_length=40, generate_id="ds", unique=True)
    name = StringField(max_length=255, unique_with="domain_id")
    state = StringField(
        max_length=20, default="ENABLED", choices=("ENABLED", "DISABLED")
    )
    data_source_type = StringField(max_length=20, choices=("LOCAL", "EXTERNAL"))
    secret_type = StringField(
        max_length=32,
        default="MANUAL",
        choices=("MANUAL", "USE_SERVICE_ACCOUNT_SECRET"),
    )
    secret_filter = EmbeddedDocumentField(SecretFilter, default=None, null=True)
    permissions = DictField(default=None, null=True)
    provider = StringField(max_length=40, default=None, null=True)
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    template = DictField(default={})
    tags = DictField(default={})
    cost_tag_keys = ListField(StringField())
    cost_additional_info_keys = ListField(StringField())
    cost_data_keys = ListField(StringField())
    data_source_account_count = IntField(default=0, min_value=0)
    connected_workspace_count = IntField(default=0, min_value=0)
    resource_group = StringField(
        max_length=255, default=None, null=True, choices=("DOMAIN", "WORKSPACE")
    )
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    last_synchronized_at = DateTimeField(default=None, null=True)

    meta = {
        "updatable_fields": [
            "name",
            "state",
            "permissions",
            "plugin_info",
            "secret_filter",
            "template",
            "tags",
            "updated_at",
            "last_synchronized_at",
            "cost_tag_keys",
            "cost_additional_info_keys",
            "cost_data_keys",
            "data_source_account_count",
            "connected_workspace_count",
        ],
        "minimal_fields": [
            "data_source_id",
            "workspace_id",
            "name",
            "state",
            "data_source_type",
            "secret_type",
            "provider",
        ],
        "ordering": ["name"],
        "indexes": [
            "state",
            "data_source_type",
            "provider",
            "resource_group",
            "workspace_id",
            "domain_id",
        ],
    }
