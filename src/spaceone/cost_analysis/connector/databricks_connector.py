import logging
from decimal import Decimal
from typing import Dict, Any, List
from collections.abc import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, DatabaseError, SQLAlchemyError
from sqlalchemy.engine import Result
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import threading

from spaceone.core import config
from spaceone.core.connector import BaseConnector
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.model.cost_model import Cost
from spaceone.cost_analysis.connector.databricks_sql_builder import DatabricksSQLBuilder

__all__ = ["DatabricksConnector"]

_LOGGER = logging.getLogger(__name__)

REQUEST_EXCLUDE_FIELDS = ["cost_id", "data_source_id", "job_id", "job_task_id", "v_workspace_id", "target"]
RESPONSE_EXCLUDE_FIELDS = ['database', 'dt', 'usageaccountid', 'payeraccountid', 'total_count']

class DatabricksConnector(BaseConnector):
    _engine = None
    _lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        warehouse_config = config.get_global("WAREHOUSES")
        warehouse_type = kwargs.get("warehouse_type")

        self.sql_builder = DatabricksSQLBuilder
        self.databricks_config = warehouse_config.get(warehouse_type)

        self._init_engine()

    def _init_engine(self):
        with self._lock:
            if not self._engine:
                _LOGGER.info("Databricks engine initializing")

                access_token = self.databricks_config.get("access_token")
                server_hostname = self.databricks_config.get("server_hostname")
                http_path = self.databricks_config.get("http_path")
                pool_size = self.databricks_config.get("pool_size")
                max_overflow = self.databricks_config.get("max_overflow")
                pool_recycle = self.databricks_config.get("pool_recycle")
                pool_timeout = self.databricks_config.get("pool_timeout")
                transport_command_timeout = self.databricks_config.get("transport_command_timeout")

                dbrx_url = (
                    f"databricks://token:{access_token}@{server_hostname}"
                    f"?http_path={http_path}"
                )

                self._engine = create_engine(
                    dbrx_url,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_recycle=pool_recycle,                       # Recycle connections every 5 minutes
                    pool_timeout=pool_timeout,
                    pool_pre_ping=True,
                    connect_args = {
                        "_transport_command_timeout": transport_command_timeout   # Server-side timeout
                    },
                )
                _LOGGER.info("Databricks engine initialized with connection pool")

    def _generate_analyze_sql(self, query: Dict, table: str) -> str:
        try:
            sql_builder = self.sql_builder(query, table)
            raw_sql = sql_builder.build_analyze_sql()
            return raw_sql
        except Exception as e:
            logging.error(f"SQL 생성 실패: {str(e)}")
            raise

    def _generate_search_sql(self, query: Dict, table: str) -> str:
        try:
            sql_builder = self.sql_builder(query, table)
            raw_sql = sql_builder.build_search_sql()
            return raw_sql
        except Exception as e:
            logging.error(f"SQL 생성 실패: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, max=10),
           retry=retry_if_exception_type(SQLAlchemyError))
    def analyze_costs(self, provider: str, query: dict):
        if "fields" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="fields")

        if "granularity" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="granularity")

        granularity = query["granularity"]

        provider_config = self.databricks_config.get(provider, {})
        catalog = provider_config.get("catalog")
        schema = provider_config.get("schema")
        table = provider_config.get("table", {}).get(granularity)

        full_table_name = f"{catalog}.{schema}.{table}"

        # EXCLUDE_FIELDS에 속한 필드는 제거.
        filtered_query = self._preprocess_query_dict(query, REQUEST_EXCLUDE_FIELDS)

        sql_query = self._generate_analyze_sql(filtered_query, full_table_name)
        _LOGGER.info(f"sql_query: {sql_query}")

        try:
            with self._engine.connect() as conn:  # 풀에서 연결 자동 할당
                _LOGGER.info(f"Pool stats: {self._engine.pool.status()}")

                dbrx_result = conn.execute(text(sql_query))

                formatted_result = self._format_like_mongodb(dbrx_result, filtered_query)
                return formatted_result

        except ProgrammingError as e:
            _LOGGER.error(f"SQL syntax error: {str(e)}", exc_info=True)
            raise
        except DatabaseError as e:
            _LOGGER.error(f"Database error: {str(e)}", exc_info=True)
            raise
        except SQLAlchemyError as e:
            _LOGGER.error(f"Query execution error: {str(e)}", exc_info=True)
            raise

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, max=10),
           retry=retry_if_exception_type(SQLAlchemyError))
    def list_costs(self, provider: str, query: dict):
        provider_config = self.databricks_config.get(provider, {})
        catalog = provider_config.get("catalog")
        schema = provider_config.get("schema")
        table = provider_config.get("table", {}).get("DAILY")

        full_table_name = f"{catalog}.{schema}.{table}"

        # EXCLUDE_FIELDS에 속한 필드는 제거.
        filtered_query = self._preprocess_query_dict(query, REQUEST_EXCLUDE_FIELDS)

        sql_query = self._generate_search_sql(filtered_query, full_table_name)
        _LOGGER.info(f"sql_query: {sql_query}")

        try:
            with self._engine.connect() as conn:  # 풀에서 연결 자동 할당
                _LOGGER.info(f"Pool stats: {self._engine.pool.status()}")

                dbrx_result = conn.execute(text(sql_query))

                return self._format_like_mongodb(dbrx_result, filtered_query)

        except ProgrammingError as e:
            _LOGGER.error(f"SQL syntax error: {str(e)}", exc_info=True)
            raise
        except DatabaseError as e:
            _LOGGER.error(f"Database error: {str(e)}", exc_info=True)
            raise
        except SQLAlchemyError as e:
            _LOGGER.error(f"Query execution error: {str(e)}", exc_info=True)
            raise
    
    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, max=10),
           retry=retry_if_exception_type(SQLAlchemyError))
    def stat_costs(self, provider: str, query: dict):
        provider_config = self.databricks_config.get(provider, {})
        catalog = provider_config.get("catalog")
        schema = provider_config.get("schema")
        table = provider_config.get("table", {}).get("MONTHLY")
        
        full_table_name = f"{catalog}.{schema}.{table}"

        # EXCLUDE_FIELDS에 속한 필드는 제거.
        filtered_query = self._preprocess_query_dict(query, REQUEST_EXCLUDE_FIELDS)

        sql_query = self._generate_search_sql(filtered_query, full_table_name)
        _LOGGER.info(f"sql_query: {sql_query}")

        try:
            with self._engine.connect() as conn:  # 풀에서 연결 자동 할당
                _LOGGER.info(f"Pool stats: {self._engine.pool.status()}")

                dbrx_result = conn.execute(text(sql_query))

                return self._format_like_mongodb(dbrx_result, filtered_query)

        except ProgrammingError as e:
            _LOGGER.error(f"SQL syntax error: {str(e)}", exc_info=True)
            raise
        except DatabaseError as e:
            _LOGGER.error(f"Database error: {str(e)}", exc_info=True)
            raise
        except SQLAlchemyError as e:
            _LOGGER.error(f"Query execution error: {str(e)}", exc_info=True)
            raise

    def _preprocess_query_dict(self, query_dict, exclude_list):
        """
        주어진 쿼리 딕셔너리에서 exclude_list에 포함된 필드를 제거.
        """
        if not query_dict:
            return query_dict

        processed_query = query_dict.copy()
        exclude_set = set(exclude_list)

        # 공통 필드 제거 로직
        def remove_excluded_fields(items: List[str]) -> List[str]:
            return [item for item in items if item not in exclude_set]

        def remove_excluded_by_key(items: List[Dict], key_field: str = 'k') -> List[Dict]:
            return [item for item in items if item.get(key_field) not in exclude_set]

        # 1. group_by 처리
        if ('group_by' in processed_query and 
            isinstance(processed_query['group_by'], list) and 
            processed_query['group_by']
        ):
            # list의 첫번째 아이템을 보고 데이터 형식 판별
            first_item = processed_query['group_by'][0]
            
            # Object 형식: [{'key': '...', 'name': '...'}]
            if isinstance(first_item, dict):
                # 'key' 값을 기준으로 exclude
                processed_query['group_by'] = [
                    item for item in processed_query['group_by']
                    if isinstance(item, dict) and item.get('key') not in exclude_set
                ]
            
            # String 형식: ['item1', 'item2']
            elif isinstance(first_item, str):
                processed_query['group_by'] = remove_excluded_fields(processed_query['group_by'])

        # 2. fields 처리
        if 'fields' in processed_query and isinstance(processed_query['fields'], dict):
            processed_query['fields'] = {
                name: config for name, config in processed_query['fields'].items()
                if config.get('key') not in exclude_set
            }

        # 3. sort 처리
        if 'sort' in processed_query and isinstance(processed_query['sort'], list):
            valid_total_fields = {f"_total_{name}" for name in processed_query.get('fields', {})}
            processed_query['sort'] = [
                item for item in processed_query['sort']
                if item.get('key') not in exclude_set and
                   (not item.get('key').startswith('_total_') or item.get('key') in valid_total_fields)
            ]

        # 4. filter 및 filter_or 처리
        for filter_key in ['filter', 'filter_or']:
            if filter_key in processed_query and isinstance(processed_query[filter_key], list):
                processed_query[filter_key] = remove_excluded_by_key(processed_query[filter_key])

        # 5. field_group 처리
        if 'field_group' in processed_query and isinstance(processed_query['field_group'], list):
            processed_query['field_group'] = remove_excluded_fields(processed_query['field_group'])
            if not processed_query['field_group']:
                # field_group이 비면 SQL 의미가 달라질 수 있으므로 로그 또는 예외 처리
                processed_query.pop('field_group', None)

        # 6. select 처리
        if 'select' in processed_query and isinstance(processed_query['select'], dict):
            new_select = {}
            for alias, definition in processed_query['select'].items():
                if alias in exclude_set:
                    continue

                is_valid = True
                if isinstance(definition, str):
                    is_valid = definition not in exclude_set
                elif isinstance(definition, dict) and 'fields' in definition:
                    is_valid = not any(
                        f in exclude_set for f in definition.get('fields', []) if isinstance(f, str)
                    )

                if is_valid:
                    new_select[alias] = definition

            if new_select:
                processed_query['select'] = new_select
            else:
                processed_query.pop('select', None)

        return processed_query

    def _format_like_mongodb(self, sql_result: Result, query_dict: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Databricks SQL 쿼리 결과를 최종 JSON 형태로 변환.
        - analyze_query: more 플래그 처리
        - search_query: total_count 처리
        """

        # 모든 중첩 구조에서 Decimal을 float으로 재귀적으로 변환
        def _decimal_to_float(item: Any) -> Any:
            # 딕셔너리 처리
            if isinstance(item, dict):
                return {k: _decimal_to_float(v) for k, v in item.items()}
            # list, tuple, NdArrayItemsContainer 등 모든 리스트 형태 컨테이너 처리
            # (단, 문자열(str, bytes)은 반복 가능하지만 분해하지 않음)
            if isinstance(item, Iterable) and not isinstance(item, (str, bytes)):
                return [_decimal_to_float(i) for i in item]
            # Decimal 타입은 float으로 변환
            if isinstance(item, Decimal):
                return float(item)
            # 그 외 타입은 그대로 반환
            return item

        # 1. SQLAlchemy Result를 dict 리스트로 변환
        results_as_dicts = [row._asdict() for row in sql_result]

        # 2. 결과 리스트 전체에 대해 재귀적으로 Decimal 변환을 적용
        # 이렇게 하면 field_group으로 생성된 중첩 배열 내부의 Decimal 값도 float으로 변환
        processed_results = [_decimal_to_float(row) for row in results_as_dicts]

        # 3.1. distinct 처리
        if query_dict.get('distinct'):
            if not processed_results:
                return {"results": []}
            # 객체 리스트 [ {'Values': v1}, ... ] 를 값 리스트 [ v1, ... ] 로 변환
            unwrapped_list = [row['Values'] for row in processed_results]
            return {"results": unwrapped_list}

        # 3.2. search_query 결과 처리 (total_count) ---
        if processed_results and 'total_count' in processed_results[0]:
            # 첫 번째 행에서 total_count를 추출
            total_count = processed_results[0]['total_count']
            cost_vo_list = []
            for row in processed_results:
                # Cost 객체 생성 시 total_count, dt, database, payeraccountid 필드는 제외
                cost_data = {k: v for k, v in row.items() if k not in RESPONSE_EXCLUDE_FIELDS}
                cost_vo_list.append(Cost(**cost_data))
            return cost_vo_list, total_count

        # 3.3. analyze_query 결과 처리 (more) ---
        page_info = query_dict.get('page', {})
        if page_info:
            # query.page 가 있는 경우에 more_flag를 함께 리턴해야 한다
            original_limit = int(page_info.get('limit', 0))

            # 3.3.1. 'more' 플래그를 확인하고, 필요한 경우 추가로 가져온 항목을 제거
            more_flag = False
            final_results_list = processed_results

            # 3.3.2. 사용자가 limit을 요청했고, 실제로 요청한 것보다 많은 결과가 반환되었는지 확인
            if original_limit > 0 and len(processed_results) > original_limit:
                more_flag = True
                # 'more' 확인용으로 가져온 마지막 항목을 결과 리스트에서 제거
                final_results_list = processed_results[:original_limit]

            # 4. 최종 형식에 맞춰 'results'와 'more' 플래그를 함께 래핑하여 반환
            return {"results": final_results_list, "more": more_flag}
        else:
            # 4. 최종 형식에 맞춰 래핑하여 반환
            return {"results": processed_results}
