
from typing import Any

from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict

from connector_postgresql.base_command import BaseCommand


class SelectValuesV2(BaseCommand, ConnectorCommand):

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
        where_clause, values = self.build_where_clause(self.schema)

        # TODO: build properly with SQL().format(Identifier(name))
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        sql = f"SELECT {columns} FROM {self.table_name} {where_clause};"  # noqa: S608

        return self.fetchall(sql, self.database_connection_str, values)
