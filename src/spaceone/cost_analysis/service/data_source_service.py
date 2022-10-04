import logging
from datetime import datetime, timedelta

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.job_model import Job
from spaceone.cost_analysis.manager.repository_manager import RepositoryManager
from spaceone.cost_analysis.manager.secret_manager import SecretManager
from spaceone.cost_analysis.manager.data_source_plugin_manager import DataSourcePluginManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.job_manager import JobManager
from spaceone.cost_analysis.manager.job_task_manager import JobTaskManager
from spaceone.cost_analysis.model.data_source_model import DataSource

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.ds_plugin_mgr: DataSourcePluginManager = self.locator.get_manager('DataSourcePluginManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'data_source_type', 'domain_id'])
    def register(self, params):
        """Register data source

        Args:
            params (dict): {
                'name': 'str',
                'data_source_type': 'str',
                'provider': 'str',
                'template': 'dict',
                'plugin_info': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        domain_id = params['domain_id']
        data_source_type = params['data_source_type']

        if data_source_type == 'EXTERNAL':
            plugin_info = params.get('plugin_info', {})

            self._validate_plugin_info(plugin_info)
            self._check_plugin(plugin_info['plugin_id'], domain_id)

            # Update metadata
            endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)
            if updated_version:
                params['plugin_info']['version'] = updated_version

            options = params['plugin_info'].get('options', {})

            plugin_metadata = self._init_plugin(endpoint, options, domain_id)

            params['plugin_info']['metadata'] = plugin_metadata

            secret_data = plugin_info.get('secret_data')
            if secret_data:
                self._verify_plugin(endpoint, plugin_info, domain_id)

                secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
                secret_id = secret_mgr.create_secret(domain_id, secret_data, plugin_info.get('schema'))

                params['plugin_info']['secret_id'] = secret_id
                del params['plugin_info']['secret_data']

        else:
            params['plugin_info'] = None

        if 'template' in params:
            # check template
            pass

        data_source_vo: DataSource = self.data_source_mgr.register_data_source(params)

        if data_source_type == 'EXTERNAL':
            data_source_id = data_source_vo.data_source_id
            metadata = data_source_vo.plugin_info.metadata

            self.ds_plugin_mgr.create_data_source_rules_by_metadata(metadata, data_source_id, domain_id)

            # TODO: set template from plugin metadata

        return data_source_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def update(self, params):
        """Update data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'template': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """
        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if 'template' in params:
            # check template
            pass

        return self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def enable(self, params):
        """ Enable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'ENABLED'}, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def disable(self, params):
        """ Disable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'DISABLED'}, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def deregister(self, params):
        """Deregister data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.plugin_info:
            secret_id = data_source_vo.plugin_info.secret_id

            if secret_id:
                secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
                secret_mgr.delete_secret(secret_id, domain_id)

        self.data_source_mgr.deregister_data_source_by_vo(data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def sync(self, params):
        """Sync data with data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'start': 'datetime',
                'no_preload_cache': 'bool',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        job_mgr: JobManager = self.locator.get_manager('JobManager')
        job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        job_options = {
            'no_preload_cache': params.get('no_preload_cache', False),
            'start': params.get('start')
        }

        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.state == 'DISABLED':
            raise ERROR_DATA_SOURCE_STATE(data_source_id=data_source_id)

        if data_source_vo.data_source_type == 'LOCAL':
            raise ERROR_NOT_ALLOW_SYNC_COMMAND(data_source_id=data_source_id)

        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(data_source_vo)
        secret_id = data_source_vo.plugin_info.secret_id
        options = data_source_vo.plugin_info.options
        schema = data_source_vo.plugin_info.schema
        secret_data = self._get_secret_data(secret_id, domain_id)

        self._check_duplicate_job(data_source_id, domain_id, job_mgr)

        params['last_synchronized_at'] = data_source_vo.last_synchronized_at

        self.ds_plugin_mgr.initialize(endpoint)
        tasks, changed = self.ds_plugin_mgr.get_tasks(options, secret_data, schema, params, domain_id)

        _LOGGER.debug(f'[sync] get_tasks: {tasks}')
        _LOGGER.debug(f'[sync] changed: {changed}')

        job_vo = job_mgr.create_job(data_source_id, domain_id, job_options, len(tasks), changed)

        if len(tasks) > 0:
            for task in tasks:
                job_task_vo = None
                task_options = task['task_options']
                try:
                    job_task_vo = job_task_mgr.create_job_task(job_vo.job_id, data_source_id, domain_id, task_options)
                    job_task_mgr.push_job_task({
                        'task_options': task_options,
                        'job_task_id': job_task_vo.job_task_id,
                        'domain_id': domain_id
                    })
                except Exception as e:
                    if job_task_vo:
                        job_task_mgr.change_error_status(job_task_vo, e)
        else:
            job_vo = job_mgr.change_success_status(job_vo)
            self.data_source_mgr.update_data_source_by_vo({'last_synchronized_at': job_vo.created_at}, data_source_vo)

        return job_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def verify_plugin(self, params):
        """ Verify data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo: DataSource = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.data_source_type == 'LOCAL':
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        endpoint = self.ds_plugin_mgr.get_data_source_plugin_endpoint_by_vo(data_source_vo)
        plugin_info = data_source_vo.plugin_info.to_dict()

        self._verify_plugin(endpoint, plugin_info, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def update_plugin(self, params):
        """Update data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',
                'version': 'str',
                'options': 'dict',
                'upgrade_mode': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        version = params.get('version')
        options = params.get('options')
        upgrade_mode = params.get('upgrade_mode')

        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if data_source_vo.data_source_type == 'LOCAL':
            raise ERROR_NOT_ALLOW_PLUGIN_SETTINGS(data_source_id=data_source_id)

        plugin_info = data_source_vo.plugin_info.to_dict()

        if version:
            plugin_info['version'] = version

        if isinstance(options, dict):
            plugin_info['options'] = options

        if upgrade_mode:
            plugin_info['upgrade_mode'] = upgrade_mode

        endpoint, updated_version = self.ds_plugin_mgr.get_data_source_plugin_endpoint(plugin_info, domain_id)
        if updated_version:
            plugin_info['version'] = updated_version

        options = plugin_info.get('options', {})
        plugin_metadata = self._init_plugin(endpoint, options, domain_id)
        plugin_info['metadata'] = plugin_metadata

        params = {
            'plugin_info': plugin_info
        }

        # TODO: set template from plugin metadata

        data_source_vo = self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

        self.ds_plugin_mgr.delete_data_source_rules(data_source_id, domain_id)
        self.ds_plugin_mgr.create_data_source_rules_by_metadata(plugin_metadata, data_source_id, domain_id)

        return data_source_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def get(self, params):
        """ Get data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']

        return self.data_source_mgr.get_data_source(data_source_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['data_source_id', 'name', 'state', 'data_source_type', 'provider', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name'])
    def list(self, params):
        """ List data sources

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'state': 'str',
                'cost_analysis_type': 'str',
                'provider': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            data_source_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.data_source_mgr.list_data_sources(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.data_source_mgr.stat_data_sources(query)

    @staticmethod
    def _validate_plugin_info(plugin_info):
        if 'plugin_id' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.plugin_id')

        if plugin_info.get('upgrade_mode', 'AUTO') == 'MANUAL' and 'version' not in plugin_info:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.version')

    def _check_plugin(self, plugin_id, domain_id):
        repo_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')
        repo_mgr.get_plugin(plugin_id, domain_id)

    def _init_plugin(self, endpoint, options, domain_id):
        self.ds_plugin_mgr.initialize(endpoint)
        return self.ds_plugin_mgr.init_plugin(options, domain_id)

    def _verify_plugin(self, endpoint, plugin_info, domain_id):
        options = plugin_info.get('options', {})
        secret_id = plugin_info.get('secret_id')
        secret_data = plugin_info.get('secret_data')
        schema = plugin_info.get('schema')

        if not secret_data:
            secret_data = self._get_secret_data(secret_id, domain_id)

        self.ds_plugin_mgr.initialize(endpoint)
        self.ds_plugin_mgr.verify_plugin(options, secret_data, schema, domain_id)

    def _get_secret_data(self, secret_id, domain_id):
        secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        if secret_id:
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
        else:
            secret_data = {}

        return secret_data

    @staticmethod
    def _check_duplicate_job(data_source_id, domain_id, job_mgr: JobManager):
        job_vos = job_mgr.filter_jobs(data_source_id=data_source_id, domain_id=domain_id, status='IN_PROGRESS')

        duplicate_job_time = datetime.utcnow() - timedelta(minutes=1)

        for job_vo in job_vos:
            if job_vo.created_at >= duplicate_job_time:
                raise ERROR_DUPLICATE_JOB(data_source_id=data_source_id)
            else:
                job_mgr.change_canceled_status(job_vo)
