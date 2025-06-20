import logging
import re
import calendar
from typing import Optional, List, Dict, Any, Callable, Tuple, Set
from decimal import Decimal

__all__ = ["DatabricksSQLBuilder"]

_LOGGER = logging.getLogger(__name__)


class DatabricksSQLBuilder:
    """
    SPO 타입의 쿼리를 Databricks SQL 쿼리로 빌드.
    """
    CTE_BASE_QUERY_NAME = "base_query"
    FG_QUERY_NAME = "fg_query"
    GRANULARITY_DATE_ALIAS = "date"

    def __init__(self, query_dict: Dict[str, Any], table_name: str):
        self.query = query_dict
        self.with_select_expressions: List[str] = []
        self.with_aliases_map: Dict[str, Dict[str, Any]] = {}
        self.where_conditions_with: List[str] = []
        self.is_group_by_needed: bool = False
        self.from_clause_with: str = table_name
        self.unwind_clauses: List[str] = []

    def build_sql(self) -> str:
        """전체 SQL 쿼리를 구성하고 반환."""
        # 1. base_query CTE 구성
        self._build_with_select_expressions()
        self._build_where_conditions()
        self._build_unwind()
        self._build_group_by()
        base_query_sql = self._assemble_base_query()

        final_query_parts = [f"WITH {self.CTE_BASE_QUERY_NAME} AS (\n{base_query_sql}\n)"]

        # 2. field_group 존재 여부에 따라 쿼리 경로 분기
        if self.query.get('field_group'):
            # --- field_group이 있으면 ---
            partition_keys, field_group_keys = self._get_fg_key_sets()

            # 2a. fg_query CTE 추가
            fg_query_sql = self._build_fg_query(partition_keys=partition_keys)
            final_query_parts.append(f", {self.FG_QUERY_NAME} AS (\n{fg_query_sql}\n)")

            # 2b. 최종 SELECT 문 구성
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

    # --- 1. base_query 구성 헬퍼 메서드 ---
    def _assemble_base_query(self) -> str:
        clauses = []
        unique_with_selects = sorted(list(set(self.with_select_expressions)))
        clauses.append(f"  SELECT {', '.join(unique_with_selects)}" if unique_with_selects else "  SELECT *")
        from_sql = f"  FROM {self.from_clause_with}"
        if self.unwind_clauses:
            from_sql += "\n  " + "\n  ".join(self.unwind_clauses)
        clauses.append(from_sql)
        if self.where_conditions_with:
            clauses.append(f"  WHERE {' AND '.join(self.where_conditions_with)}")
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

        for col_name in self.query.get('group_by', []):
            col_accessor, internal_alias, display_alias = self._format_column_accessor(col_name, for_select_alias=True)
            self._add_to_with_select(internal_alias, col_accessor, display_alias=display_alias)

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


    def _build_where_conditions(self):
        if start_date := self.query.get('start'):
            if end_date := self.query.get('end'):
                if dt_start := self._get_dt_format(start_date):
                    if dt_end := self._get_dt_format(end_date): self.where_conditions_with.append(
                        f"dt BETWEEN '{dt_start}' AND '{dt_end}'")

                if granularity := self.query.get('granularity'):
                    filter_col = 'billed_month' if granularity == 'MONTHLY' else 'billed_date'
                    if f_start := self._format_date_for_filter(start_date, granularity, True):
                        if f_end := self._format_date_for_filter(end_date, granularity,
                                                                 False): self.where_conditions_with.append(
                            f"`{filter_col}` BETWEEN '{f_start}' AND '{f_end}'")

        if filter_items := self.query.get('filter'):
            conditions = [self._build_single_filter_condition(item) for item in filter_items if item]

            if valid_conditions := [c for c in conditions if c]: self.where_conditions_with.append(
                f"({' AND '.join(valid_conditions)})")

        if filter_or_items := self.query.get('filter_or'):
            conditions = [self._build_single_filter_condition(item) for item in filter_or_items if item]

            if valid_conditions := [c for c in conditions if c]: self.where_conditions_with.append(
                f"({' OR '.join(valid_conditions)})")

    def _build_single_filter_condition(self, cond_item: Dict[str, Any]) -> Optional[str]:
        op, value = cond_item.get('o', '').lower(), cond_item.get('v')
        key_accessor = self._format_column_accessor(cond_item['k'])
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
        if handler := op_handlers.get(op): return handler(op, key_accessor, value)
        return None

    def _build_unwind(self):
        if unwind_specs := self.query.get('unwind'):
            specs = [unwind_specs] if isinstance(unwind_specs, dict) else unwind_specs
            for spec in specs:
                if path := spec.get('path'):
                    alias = self._sanitize_name(path.split('.')[-1])
                    self._add_to_with_select(alias, alias, display_alias=alias)

    def _format_column_accessor(self, col_name_str: str, for_select_alias: bool = False, avoid_cast: bool = False):
        if '.' in col_name_str:
            parts = col_name_str.split('.', 1)
            base_col, sub_field_key = self._sanitize_name(parts[0]), parts[1]
            accessor = f"`{base_col}`['{sub_field_key}']"
            if not avoid_cast: accessor = f"CAST({accessor} AS STRING)"
            if for_select_alias: return accessor, self._sanitize_name(sub_field_key), sub_field_key
            return accessor
        else:
            if for_select_alias: return f'`{col_name_str}`', self._sanitize_name(col_name_str), col_name_str
            return f'`{col_name_str}`'

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
        partition_by_sql = ", ".join(f"b.`{key}`" for key in sorted(list(partition_keys)))

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

                sum_over = f"SUM(b.`{hidden_sum_display_alias}`) OVER (PARTITION BY {partition_by_sql})"
                count_over = f"SUM(b.`{hidden_count_display_alias}`) OVER (PARTITION BY {partition_by_sql})"

                window_expressions.append(
                    f"CASE WHEN {count_over} = 0 THEN 0 ELSE {sum_over} / {count_over} END AS {total_alias}")

            elif op == 'count':
                # count의 total은 SUM으로 계산
                window_expressions.append(
                    f"SUM(b.`{display_alias}`) OVER (PARTITION BY {partition_by_sql}) AS {total_alias}")

            elif op in ['sum', 'min', 'max']:
                # sum, min, max는 동일한 연산자 사용
                window_op = op.upper()
                window_expressions.append(
                    f"{window_op}(b.`{display_alias}`) OVER (PARTITION BY {partition_by_sql}) AS {total_alias}")

        select_list = ["b.*"] + window_expressions
        return f"  SELECT\n    {', '.join(select_list)}\n  FROM\n    {self.CTE_BASE_QUERY_NAME} b"

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
            col = 'billed_month' if granularity == 'MONTHLY' else 'billed_date'
            length = 7 if granularity == 'MONTHLY' else 10
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

            if len(parts) < 2:
                _LOGGER.warning("Invalid date format for filter: %s", date_str)
                return None

            year = int(parts[0])
            month = int(parts[1])
            # YYYY-MM-DD 형식인 경우 day 값 추출
            day = int(parts[2]) if len(parts) > 2 else None

            if granularity_type == 'MONTHLY':
                return f"{year:04d}-{month:02d}"
            elif granularity_type == 'DAILY':
                if day:
                    return f"{year:04d}-{month:02d}-{day:02d}"

                # YYYY-MM 형식으로 들어왔을 때, 시작/종료일에 따라 일(day)을 결정
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
        op_map = {'eq': '=', 'not': '!=', 'lt': '<', 'lte': '<=', 'gt': '>', 'gte': '>=', 'regex': 'RLIKE'}
        return f"{key} {op_map[op]} '{self._escape_sql_string(val)}'"

    def _handle_exists_op(self, op: str, key: str, val: Any) -> str:
        return f"{key} IS NOT NULL" if val else f"{key} IS NULL"

    def _handle_contain_op(self, op: str, key: str, val: Any) -> str:
        sql_op = "ILIKE" if op == 'contain' else "NOT ILIKE"
        return f"{key} {sql_op} '%{self._escape_sql_string(val)}%'"

    def _handle_array_op(self, op: str, key: str, val: Any) -> Optional[str]:
        if not isinstance(val, list): return None

        if op in ['in', 'not_in']:
            sql_op = 'IN' if op == 'in' else 'NOT IN'
            proc_vals = [f"'{self._escape_sql_string(item)}'" if isinstance(item, str) else str(item) for item in val]
            return f"{key} {sql_op} ({', '.join(proc_vals)})"

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
