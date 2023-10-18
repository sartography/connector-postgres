
from typing import Any

from connector_postgresql.base_command import BaseCommand
from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict


class UpdateValuesV2(BaseCommand, ConnectorCommand):

    def __init__(self,
        database_connection_str: str,
        table_name: str,
        schema: dict[str, Any]
    ):
        """__init__."""
        self.database_connection_str = database_connection_str
        self.table_name = table_name
        self.schema = schema

    def execute(self, _config: Any, _task_data: Any) -> ConnectorProxyResponseDict:
        set_clause, values = self._build_set_clause(self.schema)
        where_clause, where_values = self.build_where_clause(self.schema)

        if where_values is not None:
            values += where_values

        # TODO: build properly with SQL().format(Identifier(name))
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        sql = f"UPDATE {self.table_name} {set_clause} {where_clause};"

        return self.execute_query(sql, self.database_connection_str, values)

    def _build_set_clause(self, schema: dict[str, Any]) -> tuple[str, Any]:
        columns_to_values = schema["set"]
        columns, values = zip(*columns_to_values.items(), strict=True)
        set_columns = ", ".join(f"{c} = %s" for c in columns)

        return f"SET {set_columns}", values

