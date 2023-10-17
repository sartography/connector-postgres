
from typing import Any

from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict

from connector_postgresql.base_command import BaseCommand


class InsertValues(BaseCommand, ConnectorCommand):

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
        columns = ",".join(self.schema["columns"])
        placeholders = f"({','.join(['%s'] * len(self.schema['columns']))})"
        value_lists = self.schema["values"]

        # TODO: build properly with SQL().format(Identifier(name))
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES {placeholders};"  # noqa: S608

        return self.execute_batch(sql, self.database_connection_str, value_lists)
