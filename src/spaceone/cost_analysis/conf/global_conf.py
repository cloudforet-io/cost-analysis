# Email Settings
EMAIL_CONSOLE_DOMAIN = ""
EMAIL_SERVICE_NAME = "Cloudforet"

DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    "default": {
        "db": "cost-analysis",
        "host": "localhost",
        "port": 27017,
        "username": "",
        "password": "",
    }
}

# Cost Report Config Settings
COST_REPORT_CONFIG_DEFAULT_ISSUE_DAY = 10
COST_REPORT_DEFAULT_CURRENCY = "KRW"  # KRW | USD | JPY

# Cost Report Token Settings
COST_REPORT_TOKEN_TIMEOUT = 259200  # 3 days
COST_REPORT_DEFAULT_PERMISSIONS = [
    "cost-analysis:CostReport.read",
    "cost-analysis:CostReportData.read",
    "cost-analysis:CostReportConfig.read",
    "config:DomainConfig.read",
    "identity:Provider.read",
]

CACHES = {
    "default": {},
    "local": {
        "backend": "spaceone.core.cache.local_cache.LocalCache",
        "max_size": 128,
        "ttl": 300,
    },
}

HANDLERS = {
    # "authentication": [{
    #     "backend": "spaceone.core.handler.authentication_handler:SpaceONEAuthenticationHandler"
    # }],
    # "authorization": [{
    #     "backend": "spaceone.core.handler.authorization_handler:SpaceONEAuthorizationHandler"
    # }],
    # "mutation": [{
    #     "backend": "spaceone.core.handler.mutation_handler:SpaceONEMutationHandler"
    # }],
    # "event": []
}

# Log Settings
LOG = {
    "filters": {
        "masking": {
            "rules": {
                "DataSource.register": ["secret_data"],
                "DataSource.update_secret_data": ["secret_data"],
            }
        }
    }
}

CONNECTORS = {
    "SpaceConnector": {
        "backend": "spaceone.core.connector.space_connector:SpaceConnector",
        "endpoints": {
            "identity": "grpc://identity:50051",
            "plugin": "grpc://plugin:50051",
            "repository": "grpc://repository:50051",
            "secret": "grpc://secret:50051",
            "notification": "grpc://notification:50051",
        },
    },
    "DataSourcePluginConnector": {},
    "SMTPConnector": {
        # "host": "smtp.mail.com",
        # "port": "1234",
        # "user": "cloudforet",
        # "password": "1234",
        # "from_email": "support@cloudforet.com",
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
COST_QUERY_CACHE_TIME = 4  # Day
COST_REPORT_RUN_HOUR = 0  # Hour (UTC)
COST_REPORT_RETRY_DAYS = 7  # Day

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
