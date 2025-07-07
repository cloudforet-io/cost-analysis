import logging
import re
import calendar
import datetime
from typing import Optional, List, Dict, Any, Callable, Tuple, Set
from decimal import Decimal

__all__ = ["DatabricksSQLBuilder"]

_LOGGER = logging.getLogger(__name__)
COST_MINIMAL_FIELDS = [
    "cost",
    "provider",
    "region_code",
    "product",
    "usage_type",
    "resource",
    "billed_date",
    "tags",
    "additional_info",
    "data"
    ]

class DatabricksSQLBuilder:
    """
    SPO 타입의 쿼리를 Databricks SQL 쿼리로 빌드.
    - build_analyze_sql: 복잡한 집계 쿼리 생성
    - build_search_sql: 단순 조회 및 카운트 쿼리 생성
    """
    CTE_BASE_QUERY_NAME = "base_query"
    FG_QUERY_NAME = "fg_query"
    GRANULARITY_DATE_ALIAS = "date"

    def __init__(self, query_dict: Dict[str, Any], table_name: str):
        self.query = query_dict
        self.table_name = table_name
        self.with_select_expressions: List[str] = []
        self.with_aliases_map: Dict[str, Dict[str, Any]] = {}
        self.where_conditions_with: List[str] = []
        self.is_group_by_needed: bool = False
        self.from_clause_with: str = table_name
        self.unwind_clauses: List[str] = []

    # --- Method 1: Analyze Query ---
    def build_analyze_sql(self) -> str:
        """
        analyze_costs 요청을 받아 조건에 맞는 SQL 생성 (SPO의 Analyze Query 대응)
        """
        # 1. base_query CTE 구성
        self._build_with_select_expressions()
        where_clause_str = self._build_where_clause(is_search_query=False)
        self._build_unwind()
        self._build_group_by()
        base_query_sql = self._assemble_base_query(where_clause=where_clause_str)

        final_query_parts = [f"WITH {self.CTE_BASE_QUERY_NAME} AS (\n{base_query_sql}\n)"]

        # 2. field_group 존재 여부에 따라 쿼리 경로 분기
        if self.query.get('field_group'):
            # --- field_group이 있으면 ---
            partition_keys, field_group_keys = self._get_fg_key_sets()

            # 2.1. fg_query CTE 추가
            fg_query_sql = self._build_fg_query(partition_keys=partition_keys)
            final_query_parts.append(f", {self.FG_QUERY_NAME} AS (\n{fg_query_sql}\n)")

            # 2.2. 최종 SELECT 문 구성
            final_select_sql = self._build_final_select_for_fg_path(
                grouping_cols=partition_keys,
                field_group_cols=field_group_keys
            )
            final_query_parts.append(final_select_sql)
        else:
            # --- field_group이 없으면 ---
            final_select_sql = self._build_final_select_for_simple_path()
            final_query_parts.append(final_select_sql)

        return "\n".join(final_query_parts)

    # --- Method 2: Search Query ---
    def build_search_sql(self) -> str:
        """
        list_costs 요청을 받아 집계가 없는 단순 조회하는 SQL 생성 (SPO의 Search Query 대응)
        billed_date, billed_month 조건이 없으면 성능상 지난달 또는 이번달을 기준으로 조회한다.
        """
        # 1. WHERE 절 구성
        where_clause = self._build_where_clause(is_search_query=True)
        
        # 2. count_only 처리 
        if self.query.get('count_only'):
            return f"SELECT COUNT(*) as total_count\nFROM {self.table_name}\n{where_clause}"

        # 3. distinct 처리
        if distinct_col := self.query.get('distinct'):
            # distinct는 다른 select, pagination, sort 옵션을 무시하고 고유 값 목록만 반환
            if not isinstance(distinct_col, str) or not distinct_col:
                _LOGGER.warning("The 'distinct' value must be a non-empty string. Returning empty result.")
                return f"SELECT NULL AS Values LIMIT 0"

            # `distinct_col`이 `billed_month` 등일 경우 SUBSTRING 처리
            col_accessor = self._format_column_accessor(distinct_col)
            # `Values` 라는 고정된 별칭 사용
            return f"SELECT DISTINCT {col_accessor} AS Values\nFROM {self.table_name}\n{where_clause}"

        # 4. distinct 없는 조회 (minimal, only, *)
        select_list = []
        if self.query.get('minimal'):
            select_list.extend(f"`{col}`" for col in COST_MINIMAL_FIELDS)
        elif only_cols := self.query.get('only'):
            select_list.extend(f"`{col}`" for col in only_cols)
        else:
            select_list.append("*")
        
        # total_count를 위한 윈도우 함수 추가
        select_list.append("COUNT(*) OVER () as total_count")

        select_list_str = ", ".join(select_list)

        # 5. 정렬 및 페이지네이션 절 구성
        sort_clause = self._build_sort_clause()
        # list_costs는 more 플래그가 필요 없으므로 use_plus_one=False
        pagination_clause = self._build_pagination_clause(use_plus_one=False)

        # 6. 최종 쿼리 조립 (CROSS JOIN으로 total_count 추가)
        return f"SELECT {select_list_str}\nFROM {self.table_name}\n{where_clause}\n{sort_clause}\n{pagination_clause}"

    def _build_where_clause(self, is_search_query: bool) -> str:
        """WHERE 절 문자열을 생성하는 통합 헬퍼."""
        conditions = []

        # --- 공통 필터 로직을 먼저 처리 ---
        filter_conditions = []
        if filter_items := self.query.get('filter'):
            sub_conds = [self._build_single_filter_condition(item, is_search_query) for item in filter_items if item]
            if valid_conds := [c for c in sub_conds if c]:
                filter_conditions.append(f"({' AND '.join(valid_conds)})")

        if filter_or_items := self.query.get('filter_or'):
            sub_conds = [self._build_single_filter_condition(item, is_search_query) for item in filter_or_items if item]
            if valid_conds := [c for c in sub_conds if c]:
                filter_conditions.append(f"({' OR '.join(valid_conds)})")
        
        if is_search_query:
            # search_query일 때, billed_date/billed_month 필터가 있는지 확인
            all_filters = self.query.get('filter', []) + self.query.get('filter_or', [])
            has_date_filter = any(item.get('k') in ['billed_month', 'billed_date'] for item in all_filters)
            
            # billed_date/billed_month 필터가 없으면, 이번 달과 지난달을 기본 조건으로 추가
            if not has_date_filter:
                now = datetime.datetime.now(datetime.timezone.utc)
                
                # 이번 달 YYYYMM
                current_month_dt = now.strftime('%Y%m')
                
                # 지난달 YYYYMM 계산 (연도가 바뀌는 경우도 안전하게 처리)
                first_day_of_current_month = now.replace(day=1)
                last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
                previous_month_dt = last_day_of_previous_month.strftime('%Y%m')
                
                conditions.append(f"dt IN ('{current_month_dt}', '{previous_month_dt}')")
        else:
            # analyze_query는 start/end date 사용
            if start_date := self.query.get('start'):
                if end_date := self.query.get('end'):
                    granularity = self.query.get('granularity')
                    if granularity == 'YEARLY':
                        start_dt = f"{str(start_date).split('-')[0]}01"
                        end_dt = f"{str(end_date).split('-')[0]}12"
                        conditions.append(f"dt BETWEEN '{start_dt}' AND '{end_dt}'")
                    else: # MONTHLY/DAILY
                        if dt_start := self._get_dt_format(start_date):
                            if dt_end := self._get_dt_format(end_date):
                                conditions.append(f"dt BETWEEN '{dt_start}' AND '{dt_end}'")

                    if granularity == 'YEARLY': 
                        filter_col, length = 'billed_year', 4
                    elif granularity == 'MONTHLY': 
                        filter_col, length = 'billed_month', 7
                    else: 
                        filter_col, length = 'billed_date', 10
                    
                    if f_start := self._format_date_for_filter(start_date, granularity, True):
                        if f_end := self._format_date_for_filter(end_date, granularity, False):
                            where_accessor = f"SUBSTRING(`{filter_col}`, 1, {length})"
                            conditions.append(f"{where_accessor} BETWEEN '{f_start}' AND '{f_end}'")

        # 모든 조건을 합침
        all_conditions = conditions + filter_conditions
        return f"WHERE {' AND '.join(all_conditions)}" if all_conditions else ""

    def _build_sort_clause(self) -> str:
        """
        ORDER BY 절 문자열을 생성하는 헬퍼.
        analyze_query(list)와 search_query(dict)의 다른 sort 스펙을 모두 처리
        """
        sort_input = self.query.get('sort')
        if not sort_input:
            return ""

        sort_items = []
        # search_query 형식: {"keys": [...]}
        if isinstance(sort_input, dict):
            sort_items = sort_input.get('keys', [])
        # analyze_query 형식: [...]
        elif isinstance(sort_input, list):
            sort_items = sort_input

        if not sort_items:
            return ""

        # 공통 로직: sort_items 리스트를 사용하여 SQL 구문 생성
        sort_expressions = []
        for item in sort_items:
            # item이 유효한 dict이고 'key'를 포함하는지 확인하여 안정성 강화
            if isinstance(item, dict) and 'key' in item:
                sort_expressions.append(f"`{item['key']}` {'DESC' if item.get('desc') else 'ASC'}")

        if not sort_expressions:
            return ""

        return f"ORDER BY {', '.join(sort_expressions)}"

    def _build_pagination_clause(self, use_plus_one: bool) -> str:
        """LIMIT/OFFSET 절 문자열을 생성하는 헬퍼."""
        if page_info := self.query.get('page'):
            try:
                limit = int(page_info.get('limit', 0))
                if limit > 0:
                    sql_limit = limit + 1 if use_plus_one else limit
                    start = int(page_info.get('start', 1))
                    offset = (start - 1) * limit
                    return f"LIMIT {sql_limit}" + (f" OFFSET {offset}" if offset > 0 else "")
            except (ValueError, TypeError):
                _LOGGER.warning("Invalid pagination info: %s", page_info)
        return ""

    # --- 1. base_query 구성 헬퍼 메서드 ---
    def _assemble_base_query(self, where_clause: str) -> str:
        """WITH 절 내부의 SQL 쿼리를 조립 (WHERE 절을 인자로 받음)."""
        clauses = []
        unique_with_selects = sorted(list(set(self.with_select_expressions)))
        clauses.append(f"  SELECT {', '.join(unique_with_selects)}" if unique_with_selects else "  SELECT *")
        from_sql = f"  FROM {self.from_clause_with}"
        if self.unwind_clauses:
            from_sql += "\n  " + "\n  ".join(self.unwind_clauses)
        clauses.append(from_sql)

        # where_clause가 있는 경우에 추가
        if where_clause:
            clauses.append(f"  {where_clause}")

        if self.is_group_by_needed:
            clauses.append("  GROUP BY ALL")
        return "\n".join(clauses)

    def _build_group_by(self):
        self.is_group_by_needed = any(meta.get('is_agg') for meta in self.with_aliases_map.values())

    def _build_with_select_expressions(self):
        if 'select' in self.query:
            self._build_select_from_select_key()
        self._build_select_from_defaults()

    def _add_to_with_select(self, alias: str, expression: str, is_agg: bool = False,
                            original_operator: Optional[str] = None,
                            display_alias: Optional[str] = None, is_hidden: bool = False):
        """is_hidden 플래그를 이용하여 히든 필드를 메타데이터에 저장."""
        if alias not in self.with_aliases_map:
            sql_alias_str = f'`{display_alias}`' if display_alias is not None else alias
            select_expr = f"{expression} AS {sql_alias_str}"
            self.with_select_expressions.append(select_expr)
            self.with_aliases_map[alias] = {
                'expression': expression, 'is_agg': is_agg,
                'original_operator': original_operator,
                'display_alias': display_alias or alias,
                'is_hidden': is_hidden
            }

    def _build_select_from_select_key(self):
        granularity_expr = self._get_granularity_expression()
        for alias_name, definition in self.query.get('select', {}).items():
            internal_alias = self._sanitize_name(alias_name)
            current_expr_sql = ""

            # 1. 연산자(dict) 처리
            if isinstance(definition, dict) and 'operator' in definition:
                op = definition['operator'].lower()
                if op == 'size':
                    if key := definition.get('key'):
                        current_expr_sql = f"SIZE({self._format_column_accessor(str(key), avoid_cast=True)})"
                    else:
                        continue
                else:
                    if 'fields' not in definition: continue
                    fields = definition['fields']
                    proc_fields = [granularity_expr if str(f) == self.GRANULARITY_DATE_ALIAS and granularity_expr else f
                                   for f in fields]
                    op_sql_map = {'add': lambda f: f"({' + '.join(map(str, f))})",
                                  'subtract': lambda f: f"({f[0]} - {f[1]})",
                                  'multiply': lambda f: f"({' * '.join(map(str, f))})",
                                  'divide': lambda f: f"({f[0]} / {f[1]})", }
                    if op_handler := op_sql_map.get(op):
                        sql_fields = [self._format_column_accessor(str(f)) if isinstance(f, str) else f for f in
                                      proc_fields]
                        current_expr_sql = op_handler(sql_fields)
                    else:
                        continue

            # 2. 문자열(string) 처리: 리터럴과 컬럼명 구분
            elif isinstance(definition, str):
                # 쌍따옴표로 감싸져 있으면 리터럴 문자열로 취급
                if definition.startswith('"') and definition.endswith('"'):
                    literal_str = definition[1:-1]  # 따옴표 제거
                    current_expr_sql = f"'{self._escape_sql_string(literal_str)}'"
                # 'date' 키워드는 granularity 표현식으로 변환
                elif definition == self.GRANULARITY_DATE_ALIAS and granularity_expr:
                    current_expr_sql = granularity_expr
                # 그 외에는 컬럼명으로 취급
                else:
                    current_expr_sql = self._format_column_accessor(definition)

            # 3. 숫자(int, float) 처리: 리터럴 숫자로 취급
            elif isinstance(definition, (int, float, Decimal)):
                current_expr_sql = str(definition)

            else:
                _LOGGER.warning("Unsupported definition type for select key '%s'. Skipping.", alias_name)
                continue

            if current_expr_sql:
                self._add_to_with_select(internal_alias, current_expr_sql, display_alias=alias_name)

    def _build_select_from_defaults(self):
        """'average' 연산 시 히든 필드(sum, count)를 추가 생성."""
        if granularity_expr := self._get_granularity_expression():
            self._add_to_with_select(self.GRANULARITY_DATE_ALIAS, granularity_expr,
                                     display_alias=self.GRANULARITY_DATE_ALIAS)

        # 'group_by'의 두 가지 형식(string list, object list) 지원,
        for item in self.query.get('group_by', []):
            # 1. Object 형식: {'key': 'column_name', 'name': 'Display Alias'}
            if isinstance(item, dict):
                col_name = item.get('key')
                display_alias = item.get('name')

                if not col_name or not display_alias:
                    _LOGGER.warning("Invalid group_by item format: %s. Skipping.", item)
                    continue
                
                # 'key'로 실제 SQL 접근자(accessor)를 가져옴
                col_accessor = self._format_column_accessor(col_name)
                # 내부 참조용 별칭은 일관성을 위해 실제 컬럼명(key)을 기반으로 생성
                internal_alias = self._sanitize_name(col_name)
                
                self._add_to_with_select(internal_alias, col_accessor, display_alias=display_alias)

            # 2. String 형식: 'column_name'
            elif isinstance(item, str):
                col_accessor, internal_alias, display_alias = self._format_column_accessor(item, for_select_alias=True)
                self._add_to_with_select(internal_alias, col_accessor, display_alias=display_alias)
            
            else:
                _LOGGER.warning("Unsupported group_by item type: %s. Skipping.", type(item))
                continue

        # field_group 존재 여부를 미리 확인
        has_field_group = 'field_group' in self.query

        for alias_name, spec in self.query.get('fields', {}).items():
            op = spec['operator'].lower()
            key = spec.get('key', spec.get('k', ''))
            key_accessor = self._format_column_accessor(key)
            internal_alias = self._sanitize_name(alias_name)

            if op == 'average':
                # 1. 원래 요청된 AVG 필드 추가
                self._add_to_with_select(internal_alias,
                                         f"AVG({key_accessor})",
                                         is_agg=True,
                                         original_operator=op,
                                         display_alias=alias_name)
                if has_field_group:
                    # 2. SUM 히든 필드 추가
                    hidden_sum_alias = f"__sum_for_avg_{internal_alias}"
                    self._add_to_with_select(hidden_sum_alias,
                                             f"SUM({key_accessor})",
                                             is_agg=True,
                                             original_operator='sum',
                                             display_alias=hidden_sum_alias,
                                             is_hidden=True)
                    # 3. COUNT 히든 필드 추가
                    hidden_count_alias = f"__count_for_avg_{internal_alias}"
                    self._add_to_with_select(hidden_count_alias,
                                             (
                                                 f"COUNT(*)"
                                                 if key is None or key == '' or key == '*'
                                                 else f"COUNT({key_accessor})"
                                             ),
                                             is_agg=True,
                                             original_operator='count',
                                             display_alias=hidden_count_alias,
                                             is_hidden=True)

            else:
                # 다른 연산자들은 기존 로직대로 처리
                sql_op_map = {'sum': 'SUM', 'average': 'AVG', 'max': 'MAX', 'min': 'MIN', 'count': 'COUNT',
                              'push': 'COLLECT_LIST', 'add_to_set': 'COLLECT_SET'}
                if sql_func := sql_op_map.get(op):
                    agg_expr = (
                        f"{sql_func}(*)"
                        if op == 'count' and (key is None or key == '' or key == '*')
                        else f"{sql_func}({key_accessor})"
                    )

                    self._add_to_with_select(internal_alias, agg_expr, is_agg=True, original_operator=op,
                                             display_alias=alias_name)



    def _build_single_filter_condition(self, cond_item: Dict[str, Any], is_search_query: bool = False) -> Optional[str]:
        key = cond_item.get("key", cond_item.get("k"))
        value = cond_item.get("value", cond_item.get("v"))
        op = cond_item.get("operator", cond_item.get("o", '')).lower()

        # search_query이고 key가 billed_month, billed_date일 때 SUBSTRING, dt 컬럼에도 필터 적용
        if is_search_query and key in ['billed_month', 'billed_date']:
            # 1. 원래 컬럼에 대한 조건 생성 ( accessor가 SUBSTRING을 자동으로 처리 )
            original_col_accessor = self._format_column_accessor(key)
            original_col_sql = self._handle_simple_op(op, original_col_accessor, value)
            
            # 2. dt 컬럼에 대한 조건 생성
            dt_accessor = self._format_column_accessor('dt')
            dt_value = self._get_dt_format(str(value)) # YYYY-MM... -> YYYYMM
            dt_sql = self._handle_simple_op(op, dt_accessor, dt_value) if dt_value else None
            
            # 두 조건이 모두 유효하면 AND로 연결, 아니면 원래 조건만 반환
            if original_col_sql and dt_sql:
                return f"({original_col_sql} AND {dt_sql})"
            return original_col_sql

        key_accessor = self._format_column_accessor(key)
        op_handlers: Dict[str, Callable] = {
            'eq': self._handle_simple_op,
            'not': self._handle_simple_op,
            'lt': self._handle_simple_op,
            'lte': self._handle_simple_op,
            'gt': self._handle_simple_op,
            'gte': self._handle_simple_op,
            'regex': self._handle_simple_op,
            'exists': self._handle_exists_op,
            'contain': self._handle_contain_op,
            'not_contain': self._handle_contain_op,
            'in': self._handle_array_op,
            'not_in': self._handle_array_op,
            'contain_in': self._handle_array_op,
            'not_contain_in': self._handle_array_op,
            'regex_in': self._handle_array_op,
            'datetime_gt': self._handle_datetime_op,
            'datetime_gte': self._handle_datetime_op,
            'datetime_lt': self._handle_datetime_op,
            'datetime_lte': self._handle_datetime_op,
            'timediff_gt': self._handle_timediff_op,
            'timediff_gte': self._handle_timediff_op,
            'timediff_lt': self._handle_timediff_op,
            'timediff_lte': self._handle_timediff_op,
        }
        if handler := op_handlers.get(op):
            return handler(op, key_accessor, value)

        return None

    def _build_unwind(self):
        if unwind_specs := self.query.get('unwind'):
            specs = [unwind_specs] if isinstance(unwind_specs, dict) else unwind_specs
            for spec in specs:
                if path := spec.get('path'):
                    alias = self._sanitize_name(path.split('.')[-1])
                    self._add_to_with_select(alias, alias, display_alias=alias)

    def _format_column_accessor(self, col_name_str: str, for_select_alias: bool = False, avoid_cast: bool = False):
        # 1. 맵 하위 필드 처리
        if '.' in col_name_str:
            parts = col_name_str.split('.', 1)
            base_col, sub_field_key = self._sanitize_name(parts[0]), parts[1]
            accessor = f"`{base_col}`['{sub_field_key}']"
            if not avoid_cast:
                # data 하위 필드들은 DECIMAL(38, 16)으로 형변환하고 NULL인 경우 0으로 대체
                if base_col == 'data':
                    accessor = f"COALESCE(CAST({accessor} AS DECIMAL(38, 16)), 0)"
                else:
                    accessor = f"CAST({accessor} AS STRING)"
            if for_select_alias:
                return accessor, self._sanitize_name(sub_field_key), sub_field_key
            return accessor
        # 2. 일반 컬럼 처리
        else:
            # billed_month 컬럼은 SUBSTRING(`billed_month`, 1, 7) 처리
            if col_name_str == 'billed_month':
                accessor = f"SUBSTRING(`billed_month`, 1, 7)"
                if for_select_alias:
                    # 별칭(alias)은 'billed_month' 그대로 사용
                    return accessor, 'billed_month', 'billed_month'
                return accessor
            # billed_date 컬럼은 SUBSTRING(`billed_date`, 1, 10) 처리
            elif col_name_str == 'billed_date':
                accessor = f"SUBSTRING(`billed_date`, 1, 10)"
                if for_select_alias:
                    return accessor, 'billed_date', 'billed_date'
                return accessor
            # 그 외 다른 일반 컬럼 처리
            else:
                accessor = f'`{col_name_str}`'
                if for_select_alias:
                    return accessor, self._sanitize_name(col_name_str), col_name_str
                return accessor

    # --- 2. 최종 쿼리 구성 헬퍼 메서드 (신규/변경) ---
    def _get_fg_key_sets(self) -> Tuple[Set[str], Set[str]]:
        """field_group 경로에 필요한 키 집합(파티션용, 필드 그룹용)을 반환."""
        all_groupable_aliases = {meta['display_alias']
                                 for alias, meta in self.with_aliases_map.items()
                                 if not meta.get('is_agg')
                                 }

        field_group_keys = self.query.get('field_group', [])
        field_group_display_aliases = set()
        for col in field_group_keys:
            # field_group에 있는 키가 실제로 groupable한 키인지 확인 후 추가
            # 예: 'date'는 groupable, 'cost' 같은 집계 필드는 아님
            internal_alias = self._sanitize_name(col.split('.', 1)[1]) if '.' in col else self._sanitize_name(col)
            if meta := self.with_aliases_map.get(internal_alias):
                if not meta.get('is_agg'):
                    field_group_display_aliases.add(meta['display_alias'])
            elif col == self.GRANULARITY_DATE_ALIAS:  # 'date'는 특별 처리
                field_group_display_aliases.add(self.GRANULARITY_DATE_ALIAS)

        partition_keys = all_groupable_aliases - field_group_display_aliases
        return partition_keys, field_group_display_aliases

    def _build_fg_query(self, partition_keys: Set[str]) -> str:
        """fg_query CTE의 SQL 문자열을 생성 (count, average 특별 처리)."""

        # OVER clause를 조건부로 생성
        over_clause = "OVER ()"
        if partition_keys:
            partition_by_sql = ", ".join(f"b.`{key}`" for key in sorted(list(partition_keys)))
            over_clause = f"OVER (PARTITION BY {partition_by_sql})"

        # 윈도우 함수 표현식
        window_expressions = []
        # 히든이 아닌 집계 필드만 순회
        agg_fields_meta = {alias: meta for alias, meta in self.with_aliases_map.items() if
                           meta.get('is_agg') and not meta.get('is_hidden')}

        for internal_alias, meta in agg_fields_meta.items():
            display_alias = meta.get('display_alias', internal_alias)
            op = meta.get('original_operator')
            total_alias = f"`_total_{display_alias}`"

            if op == 'average':
                # 히든 필드들의 이름을 찾아 SUM(sum)/SUM(count)으로 전체 평균 계산
                hidden_sum_display_alias = self.with_aliases_map[f"__sum_for_avg_{internal_alias}"]['display_alias']
                hidden_count_display_alias = self.with_aliases_map[f"__count_for_avg_{internal_alias}"]['display_alias']

                # 미리 생성해둔 over_clause 사용
                sum_over = f"SUM(b.`{hidden_sum_display_alias}`) {over_clause}"
                count_over = f"SUM(b.`{hidden_count_display_alias}`) {over_clause}"

                window_expressions.append(
                    f"CASE WHEN {count_over} = 0 THEN 0 ELSE {sum_over} / {count_over} END AS {total_alias}")

            elif op == 'count':
                # count의 total은 SUM으로 계산
                window_expressions.append(
                    f"SUM(b.`{display_alias}`) {over_clause} AS {total_alias}")

            elif op in ['sum', 'min', 'max']:
                # sum, min, max는 동일한 연산자 사용
                window_op = op.upper()
                window_expressions.append(
                    f"{window_op}(b.`{display_alias}`) {over_clause} AS {total_alias}")

        # fg_query의 SELECT 목록 구성 (b.* 대신 명시적 선택) ---
        # base_query로부터 가져올 컬럼 목록 (히든 필드 제외)
        select_list_from_base = []
        for meta in self.with_aliases_map.values():
            if not meta.get('is_hidden'):
                display_alias = meta['display_alias']
                select_list_from_base.append(f"b.`{display_alias}`")

        # 최종 fg_query SELECT 목록 조합
        final_select_list = select_list_from_base + window_expressions

        return (f"  SELECT\n    {', '.join(sorted(list(set(final_select_list))))}\n"
                f"  FROM\n    {self.CTE_BASE_QUERY_NAME} b")

    def _build_final_select_for_fg_path(self, grouping_cols: Set[str], field_group_cols: Set[str]) -> str:
        """field_group이 있을 때의 최종 SELECT 문을 생성."""
        select_list = [f"c.`{col}`" for col in sorted(list(grouping_cols))]

        agg_fields = {meta['display_alias']
                      for meta in self.with_aliases_map.values()
                      if meta.get('is_agg') and not meta.get('is_hidden')
                      }
        total_cols = [f"`_total_{alias}`" for alias in sorted(list(agg_fields))]
        select_list.extend(total_cols)

        struct_fields_sql = ", ".join(f"'{col}', c.`{col}`" for col in sorted(list(field_group_cols)))
        for display_alias in sorted(list(agg_fields)):
            final_struct_sql = f"{struct_fields_sql}, 'value', c.`{display_alias}`"
            select_list.append(f"ARRAY_AGG(NAMED_STRUCT({final_struct_sql})) AS `{display_alias}`")

        clauses = [f"SELECT\n  {', '.join(select_list)}", f"FROM {self.FG_QUERY_NAME} c", "GROUP BY ALL"]

        if sort_items := self.query.get('sort'):
            sort_expressions = [f"`{item['key']}` {'DESC' if item.get('desc') else 'ASC'}" for item in sort_items]
            clauses.append(f"ORDER BY {', '.join(sort_expressions)}")
        if page_info := self.query.get('page'):
            try:
                # 요청한 limit 값을 가져옴
                limit = int(page_info.get('limit', 0))
                start = int(page_info.get('start', 0))
                if limit > 0:
                    # 'more' 플래그 확인을 위해 1개 더 조회
                    sql_limit = limit + 1
                    clauses.append(f"LIMIT {sql_limit}" + (f" OFFSET {start - 1}" if start > 1 else ""))
            except (ValueError, TypeError):
                _LOGGER.warning("Invalid pagination info: %s", page_info)

        return "\n".join(clauses)

    def _build_final_select_for_simple_path(self) -> str:
        """field_group이 없을 때의 최종 SELECT 문 (히든 필드 제외)."""
        # 히든이 아닌 필드들의 출력용 별칭만 선택
        select_list = [f"`{meta['display_alias']}`"
                       for meta in self.with_aliases_map.values()
                       if not meta.get('is_hidden')
                       ]
        clauses = [f"SELECT\n  {', '.join(sorted(select_list))}\nFROM\n  {self.CTE_BASE_QUERY_NAME}"]

        if sort_items := self.query.get('sort'):
            sort_expressions = [f"`{item['key']}` {'DESC' if item.get('desc') else 'ASC'}" for item in sort_items]
            clauses.append(f"ORDER BY {', '.join(sort_expressions)}")
        if page_info := self.query.get('page'):
            try:
                # 요청한 limit 값을 가져옴
                limit = int(page_info.get('limit', 0))
                start = int(page_info.get('start', 0))
                if limit > 0:
                    # 'more' 플래그 확인을 위해 1개 더 조회
                    sql_limit = limit + 1
                    clauses.append(f"LIMIT {sql_limit}" + (f" OFFSET {start-1}" if start > 1 else ""))
            except (ValueError, TypeError):
                _LOGGER.warning("Invalid pagination info: %s", page_info)

        return "\n".join(clauses)

    def _get_granularity_expression(self) -> Optional[str]:
        if granularity := self.query.get('granularity'):
            if granularity == 'YEARLY':
                col, length = 'billed_year', 4
            elif granularity == 'MONTHLY':
                col, length = 'billed_month', 7
            else: # DAILY
                col, length = 'billed_date', 10
            return f"SUBSTRING(`{col}`, 1, {length})"
        return None

    def _get_dt_format(self, date_str: str) -> Optional[str]:
        """
        'dt' 컬럼 필터용 날짜 형식(YYYYMM)을 반환.
        'YYYY-MM', 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SSZ' 형식을 모두 지원.
        """
        if not date_str:
            return None

        try:
            # 'T'를 기준으로 날짜 부분만 추출
            date_part = str(date_str).split('T')[0]
            # '-'를 기준으로 분리
            parts = date_part.split('-')

            # 최소 YYYY와 MM은 있어야 함
            if len(parts) >= 2:
                year = int(parts[0])
                month = int(parts[1])
                return f"{year:04d}{month:02d}"
            else:
                # YYYY-MM 형식보다 짧은 경우, 잘못된 형식으로 간주
                _LOGGER.warning("Invalid date format for dt: %s", date_str)
                return None

        except (ValueError, IndexError) as e:
            _LOGGER.warning("Could not parse date string for dt format '%s': %s", date_str, e)
            return None

    def _format_date_for_filter(self, date_str: str, granularity_type: str, is_start_date: bool) -> Optional[str]:
        """Granularity 기반 필터용 날짜 형식을 반환 (안정성 개선)."""
        if not date_str:
            return None
        try:
            date_part = str(date_str).split('T')[0]
            parts = date_part.split('-')

            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1 # YYYY 형식일 경우 월은 1로 간주
            day = int(parts[2]) if len(parts) > 2 else None

            if granularity_type == 'YEARLY':
                return f"{year:04d}"
            elif granularity_type == 'MONTHLY':
                return f"{year:04d}-{month:02d}"
            elif granularity_type == 'DAILY':
                if day:
                    return f"{year:04d}-{month:02d}-{day:02d}"
                if is_start_date:
                    return f"{year:04d}-{month:02d}-01"
                else:
                    _, last_day_of_month = calendar.monthrange(year, month)
                    return f"{year:04d}-{month:02d}-{last_day_of_month:02d}"

        except (ValueError, IndexError) as e:
            _LOGGER.warning("Could not parse date string for filter '%s': %s", date_str, e)
            return None

        return None

    def _escape_sql_string(self, value: Any) -> str:
        return str(value).replace("'", "''")

    def _handle_simple_op(self, op: str, key: str, val: Any) -> str:
        # 1. null 값 케이스를 먼저 처리
        if val is None:
            if op == 'eq':
                return f"{key} IS NULL"
            if op == 'not':
                return f"{key} IS NOT NULL"
            
            # lt, gt 등 null과 비교하는 것이 의미 없는 연산자는 무시
            _LOGGER.warning("Operator '%s' with a NULL value is not supported. Skipping condition.", op)
            return None

        # 2. null이 아닌 값에 대한 기존 처리
        op_map = {'eq': '=', 'not': '!=', 'lt': '<', 'lte': '<=', 'gt': '>', 'gte': '>=', 'regex': 'RLIKE'}
        
        if op in op_map:
            return f"{key} {op_map[op]} '{self._escape_sql_string(val)}'"
        
        # 지원하지 않는 연산자인 경우
        _LOGGER.warning("Unsupported simple operator '%s'.", op)
        return None

    def _handle_exists_op(self, op: str, key: str, val: Any) -> str:
        return f"{key} IS NOT NULL" if val else f"{key} IS NULL"

    def _handle_contain_op(self, op: str, key: str, val: Any) -> str:
        sql_op = "ILIKE" if op == 'contain' else "NOT ILIKE"
        return f"{key} {sql_op} '%{self._escape_sql_string(val)}%'"

    def _handle_array_op(self, op: str, key: str, val: Any) -> Optional[str]:
        if not isinstance(val, list): 
            return None

        if op in ['in', 'not_in']:
            # 1. null 값과 일반 값 분리
            has_null = None in val
            non_null_values = [item for item in val if item is not None]

            condition_parts = []

            # 2. null 아닌 값에 대한 IN / NOT IN 구문 생성
            if non_null_values:
                sql_op = 'IN' if op == 'in' else 'NOT IN'
                proc_vals = [
                    f"'{self._escape_sql_string(item)}'" 
                    if isinstance(item, str) else str(item) 
                    for item in non_null_values
                    ]
                condition_parts.append(f"{key} {sql_op} ({', '.join(proc_vals)})")
            
            # 3. null 값에 대한 IS NULL / IS NOT NULL 구문 생성
            if has_null:
                null_op = 'IS NULL' if op == 'in' else 'IS NOT NULL'
                condition_parts.append(f"{key} {null_op}")
            
            # 4. 생성된 구문들을 결합
            if not condition_parts:
                # 빈 리스트가 입력된 경우, 항상 false를 반환하여 아무것도 선택되지 않도록 함
                return "1 = 0" 
            
            # 'in'은 OR로, 'not_in'은 AND로 결합
            joiner = ' OR ' if op == 'in' else ' AND '
            
            if len(condition_parts) > 1:
                return f"({joiner.join(condition_parts)})"
            else:
                return condition_parts[0]

        if op in ['contain_in', 'not_contain_in', 'regex_in']:
            base_op = {'contain_in': 'ILIKE', 'not_contain_in': 'NOT ILIKE', 'regex_in': 'RLIKE'}[op]
            joiner = ' OR ' if op != 'not_contain_in' else ' AND '
            sub_conds = [
                f"{key} {base_op} "
                f"{'%' + self._escape_sql_string(item) + '%' if 'contain' in op else self._escape_sql_string(item)}"
                for item in val
            ]
            return f"({joiner.join(sub_conds)})"
        return None

    def _handle_datetime_op(self, op: str, key: str, val: Any) -> str:
        sql_op = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}[op.split('_')[1]]
        return f"CAST({key} AS TIMESTAMP) {sql_op} CAST('{self._escape_sql_string(val)}' AS TIMESTAMP)"

    def _handle_timediff_op(self, op: str, key: str, val: Any) -> str:
        sql_op = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}[op.split('_')[1]]
        timediff_sql = self._parse_timediff_string(val)
        return f"CAST({key} AS TIMESTAMP) {sql_op} {timediff_sql}"

    def _parse_timediff_string(self, timediff_str: str) -> str:
        sql_expr = timediff_str.lower().replace('now/d', "date_trunc('DAY', current_timestamp())")
        sql_expr = sql_expr.replace('now', 'current_timestamp()')
        unit_map = {'d': 'days', 'h': 'hours', 'm': 'minutes', 's': 'seconds', 'w': 'weeks'}

        def replace_interval(match):
            sign, num, unit_char = match.group(1).strip(), match.group(2), match.group(3)
            return f" {sign} interval {num} {unit_map[unit_char]}"

        return "(" + re.sub(r'([+\-])\s*(\d+)([dhmsw])', replace_interval, sql_expr) + ")"

    def _sanitize_name(self, name: str) -> str:
        name = str(name).lower()
        name = re.sub(r'[^a-z0-9_.]', '_', name)
        name = name.replace('.', '_')
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        return name
