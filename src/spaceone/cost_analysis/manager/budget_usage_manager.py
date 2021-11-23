import logging
from datetime import timedelta
from dateutil.rrule import rrule, MONTHLY

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.manager.cost_manager import CostManager
from spaceone.cost_analysis.manager.budget_manager import BudgetManager
from spaceone.cost_analysis.model.budget_usage_model import BudgetUsage
from spaceone.cost_analysis.model.budget_model import Budget

_LOGGER = logging.getLogger(__name__)


class BudgetUsageManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget_usage_model: BudgetUsage = self.locator.get_model('BudgetUsage')

    def create_budget_usages(self, budget_vo: Budget):
        # Rollback

        if budget_vo.time_unit == 'TOTAL':
            dts = [dt for dt in rrule(MONTHLY, dtstart=budget_vo.start, until=budget_vo.end)]
            limit_per_month = int(budget_vo.limit / len(dts))

            for dt in dts:
                budget_usage_data = {
                    'budget_id': budget_vo.budget_id,
                    'date': dt.strftime("%Y-%m"),
                    'usd_cost': 0,
                    'limit': limit_per_month,
                    'budget': budget_vo,
                    'project_id': budget_vo.project_id,
                    'project_group_id': budget_vo.project_group_id,
                    'domain_id': budget_vo.domain_id
                }

                self.budget_usage_model.create(budget_usage_data)

        else:
            for planned_limit in budget_vo.planned_limits:
                budget_usage_data = {
                    'budget_id': budget_vo.budget_id,
                    'date': planned_limit['date'],
                    'usd_cost': 0,
                    'limit': planned_limit.limit,
                    'budget': budget_vo,
                    'project_id': budget_vo.project_id,
                    'project_group_id': budget_vo.project_group_id,
                    'domain_id': budget_vo.domain_id
                }

                self.budget_usage_model.create(budget_usage_data)

    def update_budget_usage(self, budget_vo: Budget):
        cost_mgr: CostManager = self.locator.get_manager('CostManager')
        self._update_total_budget_usage(budget_vo, cost_mgr)
        self._update_monthly_budget_usage(budget_vo, cost_mgr)

    def filter_budget_usages(self, **conditions):
        return self.budget_usage_model.query(**conditions)

    def list_budget_usages(self, query={}):
        return self.budget_usage_model.query(**query)

    def stat_budget_usages(self, query):
        return self.budget_usage_model.stat(**query)

    def _update_total_budget_usage(self, budget_vo: Budget, cost_mgr: CostManager):
        budget_mgr: BudgetManager = self.locator.get_manager('BudgetManager')

        query = self._make_cost_stat_query(budget_vo, True)
        result = cost_mgr.stat_costs(query)
        if len(result.get('results', [])) > 0:
            total_usage_usd_cost = result['results'][0].get('usd_cost')
            if total_usage_usd_cost:
                budget_mgr.update_budget_by_vo({'total_usage_usd_cost': total_usage_usd_cost}, budget_vo)

    def _update_monthly_budget_usage(self, budget_vo: Budget, cost_mgr: CostManager):
        # Rollback

        query = self._make_cost_stat_query(budget_vo)
        result = cost_mgr.stat_costs(query)
        for cost_usage_data in result.get('results', []):
            date = cost_usage_data.get('date')
            usd_cost = cost_usage_data.get('usd_cost', 0)

            if date:
                budget_usage_vos = self.budget_usage_model.filter(date=date, budget_id=budget_vo.budget_id)
                if len(budget_usage_vos) > 0:
                    budget_usage_vo = budget_usage_vos[0]
                    budget_usage_vo.update({'usd_cost': usd_cost})

    def _make_cost_stat_query(self, budget_vo: Budget, is_accumulated=False):
        query = self._get_default_query()

        if not is_accumulated:
            if budget_vo.time_unit == 'YEARLY':
                date_format = '%Y'
            else:
                date_format = '%Y-%m'

            query['aggregate'][0]['group']['keys'].append({
                'key': 'billed_at',
                'name': 'date',
                'date_format': date_format
            })

        query['filter'].append({
            'key': 'billed_at',
            'value': budget_vo.start,
            'operator': 'gte'
        })

        query['filter'].append({
            'key': 'billed_at',
            'value': budget_vo.end + timedelta(days=1),
            'operator': 'lt'
        })

        if budget_vo.project_id:
            query['filter'].append({
                'key': 'project_id',
                'value': budget_vo.project_id,
                'operator': 'eq'
            })
        else:
            query['filter'].append({
                'key': 'project_group_id',
                'value': budget_vo.project_group_id,
                'operator': 'eq'
            })

        if budget_vo.cost_types:
            for key, values in budget_vo.cost_types.to_dict().items():
                if values:
                    query['filter'].append({
                        'key': key,
                        'value': values,
                        'operator': 'in'
                    })

        return query

    @staticmethod
    def _get_default_query():
        return {
            'aggregate': [
                {
                    'group': {
                        'keys': [],
                        'fields': [
                            {
                                'name': 'usd_cost',
                                'key': 'usd_cost',
                                'operator': 'sum'
                            }
                        ]
                    }
                }
            ],
            'filter': []
        }
