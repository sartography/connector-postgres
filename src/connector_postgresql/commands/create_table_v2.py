
from typing import Any

from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict

from connector_postgresql.base_command import BaseCommand


class CreateTableV2(BaseCommand, ConnectorCommand):

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

        columns = self._column_definitions(self.schema)
        # TODO: build properly with SQL().format(Identifier(name))
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns});"

        return self.execute_query(sql, self.database_connection_str)

    def _column_definitions(self, schema: dict[str, Any]) -> str:
        def column_definition(column: dict) -> str:
            return f"{column['name']} {column['type']}"

        column_definitions = map(column_definition, schema["column_definitions"])

        return ",".join(column_definitions)
