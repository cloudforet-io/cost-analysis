import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CostService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager('CostManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['original_cost', 'original_currency', 'data_source_id', 'domain_id'])
    @change_timestamp_value(['billed_at'], timestamp_format='iso8601')
    def create(self, params):
        """Register cost

        Args:
            params (dict): {
                'original_cost': 'float',
                'original_currency': 'str',
                'usage_quantity': 'float',
                'provider': 'str',
                'region_code': 'str',
                'category': 'str',
                'product': 'str',
                'account': 'str',
                'usage_type': 'str',
                'resource_group': 'str',
                'resource': 'str',
                'tags': 'dict',
                'additional_info': 'dict',
                'service_account_id': 'str',
                'project_id': 'str',
                'data_source_id': 'str',
                'billed_at': 'datetime',
                'domain_id': 'str'
            }

        Returns:
            cost_vo (object)
        """

        # validation check (service_account_id / project_id / data_source_id)

        params['billed_at'] = params.get('billed_at') or datetime.utcnow()

        cost_vo = self.cost_mgr.create_cost(params)

        self.cost_mgr.remove_stat_cache(params['domain_id'])

        return cost_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cost_id', 'domain_id'])
    def delete(self, params):
        """Deregister cost

        Args:
            params (dict): {
                'cost_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.cost_mgr.remove_stat_cache(params['domain_id'])

        self.cost_mgr.delete_cost(params['cost_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cost_id', 'domain_id'])
    def get(self, params):
        """ Get cost

        Args:
            params (dict): {
                'cost_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            cost_vo (object)
        """

        cost_id = params['cost_id']
        domain_id = params['domain_id']

        return self.cost_mgr.get_cost(cost_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['cost_id', 'original_currency', 'provider', 'region_code', 'region_key', 'category',
                          'product', 'account', 'usage_type', 'resource_group', 'resource', 'service_account_id',
                          'project_id', 'data_source_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['cost_id'])
    def list(self, params):
        """ List costs

        Args:
            params (dict): {
                'cost_id': 'str',
                'original_currency': 'str',
                'provider': 'str',
                'region_code': 'str',
                'region_key': 'str',
                'category': 'str',
                'product': 'str',
                'account': 'str',
                'usage_type': 'str',
                'resource_group': 'str',
                'resource': 'str',
                'service_account_id': 'str',
                'project_id': 'str',
                'data_source_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list' // from meta
            }

        Returns:
            cost_vos (object)
            total_count
        """

        query = params.get('query', {})
        query['page'] = self._set_page_limit(query.get('page'))
        query['filter'] = self._change_project_group_filter(query.get('filter', []), params['domain_id'])

        return self.cost_mgr.list_costs(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['granularity', 'start', 'end', 'domain_id'])
    def analyze(self, params):
        """
        Args:
            params (dict): {
                'granularity': 'str',
                'start': 'str',
                'end': 'str',
                'group_by': 'list',
                'filter': 'list',
                'limit': 'int',
                'page': 'dict',
                'sort': 'dict',
                'include_usage_quantity': 'bool',
                'include_others': 'bool',
                'domain_id': 'str',
                'user_projects': 'list' // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params['domain_id']
        granularity = params['granularity']
        group_by = params.get('group_by', [])
        query_filter = params.get('filter', [])
        limit = params.get('limit')
        page = self._set_page_limit(params.get('page'))
        sort = params.get('sort')
        include_usage_quantity = params.get('include_usage_quantity', False)
        include_others = params.get('include_others', False)
        has_project_group_id = 'project_group_id' in group_by

        if limit:
            if limit > 1000:
                limit = 1000

            page = None

        start = self._parse_start_time(params['start'])
        end = self._parse_end_time(params['end'])

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(reason='End date must be greater than start date.')

        if granularity in ['ACCUMULATED', 'MONTHLY']:
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(reason='Request up to a maximum of 12 months.')
        elif granularity == 'DAILY':
            if start + relativedelta(days=31) < end:
                raise ERROR_INVALID_DATE_RANGE(reason='Request up to a maximum of 31 days.')

        query_filter = self._add_domain_filter(query_filter, domain_id)

        if 'user_projects' in params:
            query_filter = self._add_user_projects_filter(query_filter, params['user_projects'])

        query_filter = self._change_project_group_filter(query_filter, domain_id)

        if granularity == 'ACCUMULATED':
            query = self.cost_mgr.make_accumulated_query(group_by, limit, query_filter, include_others,
                                                         include_usage_quantity, has_project_group_id)
        else:
            query = self.cost_mgr.make_trend_query(granularity, group_by, limit, query_filter, include_others,
                                                   include_usage_quantity, has_project_group_id)

        query_hash = utils.dict_to_hash(query)

        self.cost_mgr.create_cost_query_history(query, query_hash, granularity, start, end, domain_id)

        query = self.cost_mgr.add_date_range_filter(query, granularity, start, end)
        query_hash_with_date_range = utils.dict_to_hash(query)

        if self.cost_mgr.is_monthly_cost(granularity, start, end):
            response = self.cost_mgr.stat_monthly_costs_with_cache(query, query_hash_with_date_range, domain_id)
        else:
            response = self.cost_mgr.stat_costs_with_cache(query, query_hash_with_date_range, domain_id)

        if has_project_group_id:
            response = self.cost_mgr.sum_costs_by_project_group(response, granularity, group_by, domain_id,
                                                                include_usage_quantity)

        if include_others and limit:
            response = self.cost_mgr.sum_costs_over_limit(response, granularity, limit, include_usage_quantity)
        elif has_project_group_id and limit:
            response = self.cost_mgr.slice_results(response, limit)
        elif page:
            response = self.cost_mgr.page_results(response, page)

        return response

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['cost_id'])
    @change_date_value()
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list' // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params['domain_id']
        query = params.get('query', {})
        query['page'] = self._set_page_limit(query.get('page'))
        query['filter'] = self._change_project_group_filter(query.get('filter', []), params['domain_id'])

        query_hash = utils.dict_to_hash(query)

        if self.cost_mgr.is_monthly_stat_query(query):
            return self.cost_mgr.stat_monthly_costs_with_cache(query, query_hash, domain_id)
        else:
            return self.cost_mgr.stat_costs_with_cache(query, query_hash, domain_id)

    @staticmethod
    def _add_domain_filter(query_filter, domain_id):
        query_filter.append({
            'k': 'domain_id',
            'v': domain_id,
            'o': 'eq'
        })

        return query_filter

    @staticmethod
    def _add_user_projects_filter(query_filter, user_projects):
        query_filter.append({
            'k': 'project_id',
            'v': user_projects,
            'o': 'in'
        })

        return query_filter

    def _change_project_group_filter(self, query_filter, domain_id):
        changed_filter = []
        project_group_query = {
            'filter': [],
            'only': ['project_group_id']
        }

        for condition in query_filter:
            key = condition.get('key', condition.get('k'))
            value = condition.get('value', condition.get('v'))
            operator = condition.get('operator', condition.get('o'))

            if not all([key, operator]):
                raise ERROR_DB_QUERY(reason='filter condition should have key, value and operator.')

            if key == 'project_group_id':
                project_group_query['filter'].append(condition)
            else:
                changed_filter.append(condition)

        if len(project_group_query['filter']) > 0:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            response = identity_mgr.list_project_groups(project_group_query, domain_id)
            project_group_ids = []
            project_ids = []
            for project_group_info in response.get('results', []):
                project_group_ids.append(project_group_info['project_group_id'])

            for project_group_id in project_group_ids:
                response = identity_mgr.list_projects_in_project_group(project_group_id, domain_id, True,
                                                                       {'only': ['project_id']})
                for project_info in response.get('results', []):
                    if project_info['project_id'] not in project_ids:
                        project_ids.append(project_info['project_id'])

            changed_filter.append({'k': 'project_id', 'v': project_ids, 'o': 'in'})

        return changed_filter

    def _parse_start_time(self, date_str):
        return self._convert_date_from_string(date_str.strip(), 'start')

    def _parse_end_time(self, date_str):
        date = self._convert_date_from_string(date_str.strip(), 'end')

        if len(date_str.strip()) == 7:
            return date + relativedelta(months=1)
        else:
            return date + relativedelta(days=1)

    @staticmethod
    def _convert_date_from_string(date_str, key):
        if len(date_str) == 7:
            # Month (YYYY-MM)
            date_format = '%Y-%m'
        else:
            # Date (YYYY-MM-DD)
            date_format = '%Y-%m-%d'

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_format)

    @staticmethod
    def _set_page_limit(page: dict = None):
        page = page or {}
        limit = page.get('limit', 1000)

        if limit > 1000:
            limit = 1000

        page['limit'] = limit
        return page
