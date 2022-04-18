import logging
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.manager.identity_manager import IdentityManager
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
        if budget_vo.time_unit == 'TOTAL':
            start_dt = datetime.strptime(budget_vo.start, '%Y-%m')
            end_dt = datetime.strptime(budget_vo.end, '%Y-%m')

            dts = [dt for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt)]
            limit_per_month = int(budget_vo.limit / len(dts))

            for dt in dts:
                budget_usage_data = {
                    'budget_id': budget_vo.budget_id,
                    'name': budget_vo.name,
                    'date': dt.strftime("%Y-%m"),
                    'usd_cost': 0,
                    'limit': limit_per_month,
                    'cost_types': budget_vo.cost_types.to_dict() if budget_vo.cost_types else None,
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
                    'name': budget_vo.name,
                    'date': planned_limit['date'],
                    'usd_cost': 0,
                    'limit': planned_limit.limit,
                    'cost_types': budget_vo.cost_types.to_dict() if budget_vo.cost_types else None,
                    'budget': budget_vo,
                    'project_id': budget_vo.project_id,
                    'project_group_id': budget_vo.project_group_id,
                    'domain_id': budget_vo.domain_id
                }

                self.budget_usage_model.create(budget_usage_data)

    def update_budget_usage_by_vo(self, params, budget_usage_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_budget_usage_by_vo._rollback] Revert Data : '
                         f'{old_data["budget_id"]} / {old_data["date"]}')
            budget_usage_vo.update(old_data)

        self.transaction.add_rollback(_rollback, budget_usage_vo.to_dict())
        return budget_usage_vo.update(params)

    def update_cost_usage(self, budget_vo: Budget):
        _LOGGER.info(f'[update_cost_usage] Update Budget Usage: {budget_vo.budget_id}')
        cost_mgr: CostManager = self.locator.get_manager('CostManager')
        self._update_total_budget_usage(budget_vo, cost_mgr)
        self._update_monthly_budget_usage(budget_vo, cost_mgr)

    def filter_budget_usages(self, **conditions):
        return self.budget_usage_model.filter(**conditions)

    def list_budget_usages(self, query={}):
        return self.budget_usage_model.query(**query)

    def stat_budget_usages(self, query):
        return self.budget_usage_model.stat(**query)

    def _update_total_budget_usage(self, budget_vo: Budget, cost_mgr: CostManager):
        budget_mgr: BudgetManager = self.locator.get_manager('BudgetManager')

        query = self._make_cost_stat_query(budget_vo, True)
        result = cost_mgr.stat_monthly_costs(query)
        if len(result.get('results', [])) > 0:
            total_usage_usd_cost = result['results'][0].get('usd_cost')
            if total_usage_usd_cost:
                budget_mgr.update_budget_by_vo({'total_usage_usd_cost': total_usage_usd_cost}, budget_vo)
        else:
            budget_mgr.update_budget_by_vo({'total_usage_usd_cost': 0}, budget_vo)

    def _update_monthly_budget_usage(self, budget_vo: Budget, cost_mgr: CostManager):
        update_data = {}
        query = self._make_cost_stat_query(budget_vo)
        result = cost_mgr.stat_monthly_costs(query)
        for cost_usage_data in result.get('results', []):
            date = cost_usage_data.get('date')
            usd_cost = cost_usage_data.get('usd_cost', 0)

            if date:
                update_data[date] = usd_cost

        budget_usage_vos = self.budget_usage_model.filter(budget_id=budget_vo.budget_id)
        for budget_usage_vo in budget_usage_vos:
            if budget_usage_vo.date in update_data:
                budget_usage_vo.update({'usd_cost': update_data[budget_usage_vo.date]})
            else:
                budget_usage_vo.update({'usd_cost': 0})

    def _make_cost_stat_query(self, budget_vo: Budget, is_accumulated=False):
        query = self._get_default_query()

        if not is_accumulated:
            if budget_vo.time_unit == 'YEARLY':
                query['aggregate'][0]['group']['keys'].append({
                    'key': 'billed_year',
                    'name': 'date'
                })
            else:
                query['aggregate'][0]['group']['keys'].append({
                    'key': 'billed_month',
                    'name': 'date'
                })

        query['filter'].append({
            'key': 'billed_month',
            'value': budget_vo.start,
            'operator': 'gte'
        })

        query['filter'].append({
            'key': 'billed_month',
            'value': budget_vo.end,
            'operator': 'lte'
        })

        if budget_vo.project_id:
            query['filter'].append({
                'key': 'project_id',
                'value': budget_vo.project_id,
                'operator': 'eq'
            })
        else:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            response = identity_mgr.list_projects_in_project_group(budget_vo.project_group_id,
                                                                   budget_vo.domain_id, True)

            project_ids = []
            for project_info in response.get('results', []):
                project_ids.append(project_info['project_id'])

            query['filter'].append({
                'key': 'project_id',
                'value': project_ids,
                'operator': 'in'
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
