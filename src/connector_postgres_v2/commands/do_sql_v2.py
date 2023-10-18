
from typing import Any

from psycopg2.extensions import register_adapter  # type: ignore
from psycopg2.extras import Json  # type: ignore
from spiffworkflow_connector_command.command_interface import ConnectorCommand
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict

from connector_postgres_v2.base_command import BaseCommand

register_adapter(dict, Json)

class DoSQL(BaseCommand, ConnectorCommand):

    def __init__(self,
        database_connection_str: str,
        schema: dict[str, Any]
    ):
        """__init__."""
        self.database_connection_str = database_connection_str
        self.schema = schema

    def execute(self, _config: Any, _task_data: Any) -> ConnectorProxyResponseDict:

        sql = self.schema["sql"]
        values = self.schema.get("values", [])
        fetch_results = self.schema.get("fetch_results", False)

        if fetch_results:
            return self.fetchall(sql, self.database_connection_str, values)

        return self.execute_query(sql, self.database_connection_str, values)
