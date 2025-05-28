import logging
from datetime import datetime
from decimal import Decimal
from databricks import sql

from spaceone.core import config
from spaceone.core.connector import BaseConnector

__all__ = ["DatabricksConnector"]

_LOGGER = logging.getLogger(__name__)

EXCLUDE_FIELDS = ["data_source_id"]


class DatabricksConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.warehouse_config = None
        self.connection = None
        self.cursor = None
        self.table_name = None
        self.provider = None
        self.init(*args, **kwargs)

    def init(self, *args, **kwargs):
        self.provider = kwargs.get("provider")
        warehouse_type = kwargs.get("warehouse_type", "DATABRICKS")

        warehouse_configs = config.get_global("WAREHOUSES")
        databricks_config = warehouse_configs.get(warehouse_type)

        server_hostname = databricks_config.get("server_hostname")
        http_path = databricks_config.get("http_path")
        access_token = databricks_config.get("access_token")

        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )

        self.table_name = databricks_config.get("table_name")
        self.connection = connection
        self.cursor = connection.cursor()

    def analyze_costs(self, query: dict, table_name: str = None):
        if not table_name:
            table_name = self.table_name

        query["filter"].append(
            {"key": "provider", "operator": "eq", "value": self.provider}
        )

        converted_query = self.convert_query_to_sql(query, table_name)
        _LOGGER.debug(f"[DatabricksConnector] Converted Query: {converted_query}")

        self.cursor.execute(converted_query)

        results = []
        for row in self.cursor.fetchall():
            row_dict = {
                k: (float(v) if isinstance(v, Decimal) else v)
                for k, v in row.asDict().items()
            }
            results.append(row_dict)

        self.cursor.close()
        self.connection.close()
        return {"results": results, "total_count": len(results)}

    def convert_query_to_sql(self, query: dict, table_name: str) -> str:
        def parse_filter(filter_list):
            conditions = []
            for f in filter_list:
                key = f.get("key") or f.get("k")
                if key in EXCLUDE_FIELDS:
                    continue
                value = f.get("value") or f.get("v")
                op = f.get("operator") or f.get("o")

                if isinstance(value, str):
                    value = f"'{value}'"
                elif isinstance(value, list):
                    value = "(" + ", ".join(map(str, value)) + ")"
                else:
                    value = str(value)

                if op == "eq":
                    conditions.append(f"{key} = {value}")
                elif op == "ne":
                    conditions.append(f"{key} != {value}")
                elif op == "in":
                    conditions.append(f"{key} IN {value}")
                elif op == "nin":
                    conditions.append(f"{key} NOT IN {value}")
                elif op == "gt":
                    conditions.append(f"{key} > {value}")
                elif op == "lt":
                    conditions.append(f"{key} < {value}")
                elif op == "gte":
                    conditions.append(f"{key} >= {value}")
                elif op == "lte":
                    conditions.append(f"{key} <= {value}")
                else:
                    conditions.append(f"{key} {op} {value}")
            return conditions

        group_by = query.get("group_by", [])
        fields = query.get("fields", {})
        select = query.get("select", {})
        sort = query.get("sort", [])
        page = query.get("page", {})
        field_group = query.get("field_group", [])
        granularity = query.get("granularity")
        start = query.get("start")
        end = query.get("end")
        filters = query.get("filter", [])
        filters_or = query.get("filter_or", [])
        unwind = query.get("unwind", {})
        keyword = query.get("keyword")

        group_by_clauses = []
        select_clauses = []

        # group_by
        for item in group_by:
            if isinstance(item, str):
                key = item
                select_clauses.append(f"{key}")
            elif isinstance(item, dict):
                key = item.get("key", item.get("k"))
                name = item.get("name", key)
                select_clauses.append(f"{key} AS {name}")
            else:
                continue

            group_by_clauses.append(key)

        # granularity and field_group
        if granularity and "date" in field_group:
            date_expr = (
                "DATE_FORMAT(billed_month, 'yyyy-MM')"
                if granularity == "MONTHLY"
                else "billed_month"
            )
            select_clauses.append(f"{date_expr} AS billed_date")
            group_by_clauses.append(date_expr)

        # fields (aggregation)
        for alias, field_def in fields.items():
            agg_key = field_def["key"]
            agg_op = field_def["operator"].upper()
            select_clauses.append(f"{agg_op}({agg_key}) AS {alias}")

        # select (projection)
        for key, val in select.items():
            select_clauses.append(f"{val.get('key', key)} AS {key}")

        # unwind (flattening) - placeholder
        if unwind:
            select_clauses.insert(0, f"-- Unwind on {unwind.get('path')}")

        # keyword (text search)
        if keyword:
            filters.append({"key": "text", "operator": "like", "value": f"%{keyword}%"})

        # WHERE
        where_clauses = []
        if granularity == "MONTHLY":
            date_field_name = "billed_month"
            date_format = "%Y-%m"
        elif granularity == "DAILY":
            date_field_name = "billed_date"
            date_format = "%Y-%m-%d"
        else:
            date_field_name = "billed_year"
            date_format = "%Y"

        if start:
            start_date = datetime.strptime(start, date_format).strftime(date_format)
            where_clauses.append(f"{date_field_name} >= DATE('{start_date}')")
        if end:
            end_date = datetime.strptime(end, date_format).strftime(date_format)
            where_clauses.append(f"{date_field_name} <= DATE('{end_date}')")

        where_clauses.extend(parse_filter(filters))
        if filters_or:
            or_conditions = parse_filter(filters_or)
            if or_conditions:
                where_clauses.append("(" + " OR ".join(or_conditions) + ")")

        # ORDER BY
        order_by_clause = ""
        if sort:
            sort_key = sort[0]["key"].replace("_total_", "")
            sort_dir = "DESC" if sort[0].get("desc", False) else "ASC"
            order_by_clause = f"ORDER BY {sort_key} {sort_dir}"

        # PAGINATION
        limit = page.get("limit", 100)
        start_offset = max(page.get("start", 1) - 1, 0)
        limit_offset_clause = f"LIMIT {limit} OFFSET {start_offset}"

        # Final SQL assembly
        query_parts = [
            "SELECT",
            "    " + ",\n    ".join(select_clauses),
            f"FROM\n    {table_name}",
        ]

        if where_clauses:
            query_parts.append("WHERE\n    " + " AND\n    ".join(where_clauses))
        if group_by_clauses:
            query_parts.append("GROUP BY ALL")
        if order_by_clause:
            query_parts.append(order_by_clause)
        query_parts.append(limit_offset_clause)

        converted_query = "\n".join(query_parts)
        _LOGGER.debug(f"[convert_query_to_sql] Final SQL Query: {query_parts}")
        return converted_query
