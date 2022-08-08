import logging
import fnmatch

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.data_source_rule_manager import DataSourceRuleManager
from spaceone.cost_analysis.manager.data_source_manager import DataSourceManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
from spaceone.cost_analysis.model.data_source_rule_model import DataSourceRule

_LOGGER = logging.getLogger(__name__)

_SUPPORTED_CONDITION_KEYS = ['provider', 'region_code', 'product', 'account', 'usage_type', 'resource_group',
                             'resource', 'tags.<key>', 'additional_info.<key>']
_SUPPORTED_CONDITION_OPERATORS = ['eq', 'contain', 'not', 'not_contain']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceRuleService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_rule_mgr: DataSourceRuleManager = self.locator.get_manager('DataSourceRuleManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'conditions_policy', 'actions', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Create data source rule

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'str',
                'actions': 'dict',
                'options': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            data_source_rule_vo (object)
        """

        domain_id = params['domain_id']
        data_source_id = params['data_source_id']
        conditions = params.get('conditions', [])
        conditions_policy = params['conditions_policy']
        actions = params['actions']

        if conditions_policy == 'ALWAYS':
            params['conditions'] = []
        else:
            if len(conditions) == 0:
                raise ERROR_REQUIRED_PARAMETER(key='conditions')
            else:
                self._check_conditions(conditions)

        self._check_actions(actions, domain_id)

        data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        data_source_vo = data_source_mgr.get_data_source(data_source_id, domain_id)

        params['data_source'] = data_source_vo
        params['order'] = self._get_highest_order(data_source_id, domain_id) + 1

        return self.data_source_rule_mgr.create_data_source_rule(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_rule_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update data source rule

        Args:
            params (dict): {
                'data_source_rule_id': 'str',
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'list',
                'actions': 'dict',
                'options': 'dict'
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            data_source_rule_vo (object)
        """

        data_source_rule_id = params['data_source_rule_id']
        domain_id = params['domain_id']
        conditions_policy = params.get('conditions_policy')
        conditions = params.get('conditions', [])

        data_source_rule_vo = self.data_source_rule_mgr.get_data_source_rule(data_source_rule_id, domain_id)

        if conditions_policy:
            if conditions_policy == 'ALWAYS':
                params['conditions'] = []
            else:
                if len(conditions) == 0:
                    raise ERROR_REQUIRED_PARAMETER(key='conditions')
                else:
                    self._check_conditions(conditions)

        if 'actions' in params:
            self._check_actions(params['actions'], domain_id)

        return self.data_source_rule_mgr.update_data_source_rule_by_vo(params, data_source_rule_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_rule_id', 'order', 'domain_id'])
    def change_order(self, params):
        """ Change data source rule's order

        Args:
            params (dict): {
                'data_source_rule_id': 'str',
                'order': 'int',
                'domain_id': 'str'
            }

        Returns:
            data_source_rule_vo (object)
        """

        data_source_rule_id = params['data_source_rule_id']
        order = params['order']
        domain_id = params['domain_id']

        self._check_order(order)

        target_data_source_rule_vo: DataSourceRule = self.data_source_rule_mgr.get_data_source_rule(data_source_rule_id, \
                                                                                                    domain_id)

        if target_data_source_rule_vo.order == order:
            return target_data_source_rule_vo

        highest_order = self._get_highest_order(target_data_source_rule_vo.data_source_id,
                                                target_data_source_rule_vo.domain_id)

        if order > highest_order:
            raise ERROR_INVALID_PARAMETER(key='order',
                                          reason=f'There is no data source rules greater than the {str(order)} order.')

        data_source_rule_vos = self._get_all_data_source_rules(target_data_source_rule_vo.data_source_id,
                                                               target_data_source_rule_vo.domain_id,
                                                               target_data_source_rule_vo.data_source_rule_id)

        data_source_rule_vos.insert(order-1, target_data_source_rule_vo)

        i = 0
        for data_source_rule_vo in data_source_rule_vos:
            if target_data_source_rule_vo != data_source_rule_vo:
                self.data_source_rule_mgr.update_data_source_rule_by_vo({'order': i+1}, data_source_rule_vo)

            i += 1

        return self.data_source_rule_mgr.update_data_source_rule_by_vo({'order': order}, target_data_source_rule_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_rule_id', 'domain_id'])
    def delete(self, params):
        """Delete data source rule

        Args:
            params (dict): {
                'data_source_rule_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        data_source_rule_id = params['data_source_rule_id']
        domain_id = params['domain_id']

        data_source_rule_vo: DataSourceRule = self.data_source_rule_mgr.get_data_source_rule(data_source_rule_id,
                                                                                             domain_id)

        data_source_id = data_source_rule_vo.data_source_id

        self.data_source_rule_mgr.delete_data_source_rule_by_vo(data_source_rule_vo)

        data_source_rule_vos = self._get_all_data_source_rules(data_source_id, domain_id)

        i = 0
        for data_source_rule_vo in data_source_rule_vos:
            self.data_source_rule_mgr.update_data_source_rule_by_vo({'order': i+1}, data_source_rule_vo)
            i += 1

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_rule_id', 'domain_id'])
    def get(self, params):
        """ Get data source rule

        Args:
            params (dict): {
                'data_source_rule_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            data_source_rule_vo (object)
        """

        data_source_rule_id = params['data_source_rule_id']
        domain_id = params['domain_id']

        return self.data_source_rule_mgr.get_data_source_rule(data_source_rule_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['data_source_rule_id', 'name', 'data_source_id', 'domain_id'])
    @append_keyword_filter(['data_source_rule_id', 'name'])
    def list(self, params):
        """ List data source rule

        Args:
            params (dict): {
                'data_source_rule_id': 'str',
                'name': 'str',
                'data_source_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            data_source_rule_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.data_source_rule_mgr.list_data_source_rules(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['data_source_rule_id', 'name'])
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
        return self.data_source_rule_mgr.stat_data_source_rules(query)

    @staticmethod
    def _check_conditions(conditions):
        for condition in conditions:
            key = condition.get('key')
            value = condition.get('value')
            operator = condition.get('operator')

            if not (key and value and operator):
                raise ERROR_INVALID_PARAMETER(key='conditions', reason='Condition should have key, value and operator.')

            if key not in _SUPPORTED_CONDITION_KEYS:
                if not (fnmatch.fnmatch(key, 'additional_info.*') or fnmatch.fnmatch(key, 'tags.*')):
                    raise ERROR_INVALID_PARAMETER(key='conditions.key',
                                                  reason=f'Unsupported key. '
                                                         f'({" | ".join(_SUPPORTED_CONDITION_KEYS)})')
            if operator not in _SUPPORTED_CONDITION_OPERATORS:
                raise ERROR_INVALID_PARAMETER(key='conditions.operator',
                                              reason=f'Unsupported operator. '
                                                     f'({" | ".join(_SUPPORTED_CONDITION_OPERATORS)})')

    def _check_actions(self, actions, domain_id):
        if 'change_project' in actions:
            project_id = actions['change_project']

            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            identity_mgr.get_project(project_id, domain_id)

        if 'match_project' in actions:
            if 'source' not in actions['match_project']:
                raise ERROR_REQUIRED_PARAMETER(key='actions.match_project.source')

        if 'match_service_account' in actions:
            if 'source' not in actions['match_service_account']:
                raise ERROR_REQUIRED_PARAMETER(key='actions.match_service_account.source')

    def _get_highest_order(self, data_source_id, domain_id):
        data_source_rule_vos = self.data_source_rule_mgr.filter_data_source_rules(data_source_id=data_source_id,
                                                                                  domain_id=domain_id)

        return data_source_rule_vos.count()

    @staticmethod
    def _check_order(order):
        if order <= 0:
            raise ERROR_INVALID_PARAMETER(key='order', reason='The order must be greater than 0.')

    def _get_all_data_source_rules(self, data_source_id, domain_id, exclude_data_source_rule_id=None):
        query = {
            'filter': [
                {
                    'k': 'domain_id',
                    'v': domain_id,
                    'o': 'eq'
                },
                {
                    'k': 'data_source_id',
                    'v': data_source_id,
                    'o': 'eq'
                },
            ],
            'sort': {
                'key': 'order'
            }
        }

        if exclude_data_source_rule_id is not None:
            query['filter'].append({
                'k': 'data_source_rule_id',
                'v': exclude_data_source_rule_id,
                'o': 'not'
            })

        data_source_rule_vos, total_count = self.data_source_rule_mgr.list_data_source_rules(query)
        return list(data_source_rule_vos)
