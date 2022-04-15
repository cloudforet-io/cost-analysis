DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    'default': {
        'db': 'cost-analysis',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}

HANDLERS = {
}

CONNECTORS = {
    'SpaceConnector': {
        'backend': 'spaceone.core.connector.space_connector.SpaceConnector',
        'endpoints': {
            'identity': 'grpc://identity:50051',
            'plugin': 'grpc://plugin:50051',
            'repository': 'grpc://repository:50051',
            'secret': 'grpc://secret:50051',
            'notification': 'grpc://notification:50051'

        }
    },
    'DataSourcePluginConnector': {
    },
}

# Scheduler Settings
QUEUES = {}
SCHEDULERS = {}
WORKERS = {}
TOKEN = ""
TOKEN_INFO = {}

# Job Settings
JOB_TIMEOUT = 600
DATA_SOURCE_SYNC_HOUR = 16  # Hour (UTC)
COST_QUERY_CACHE_TIME = 7   # Day

DEFAULT_EXCHANGE_RATE = {
    # 'KRW': 1178.7,
    # 'JPY': 114,
    # 'CNY': 6.3
}

INSTALLED_DATA_SOURCE_PLUGINS = [
    # {
    #     'name': '',
    #     'data_source_type': 'EXTERNAL',
    #     'plugin_info': {
    #         'plugin_id': '',
    #         'version': '',
    #         'options': {},
    #         'secret_data': {},
    #         'schema': '',
    #         'upgrade_mode': ''
    #     },
    #     'tags':{
    #         'description': ''
    #     }
    # }
]
