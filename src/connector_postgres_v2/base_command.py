from typing import Any

import psycopg2  # type: ignore
from spiffworkflow_connector_command.command_interface import CommandErrorDict
from spiffworkflow_connector_command.command_interface import CommandResponseDict
from spiffworkflow_connector_command.command_interface import ConnectorProxyResponseDict


class ConnectionConfig:
    def __init__(self, database: str, host: str, port: int, username: str, password: str):
        self.database = database
        self.host = host
        self.port = port
        self.user = username
        self.password = password

class BaseCommand:
    def _execute(self, sql: str, conn_str: str, handler: Any) -> ConnectorProxyResponseDict:
        conn = None
        error: CommandErrorDict | None = None
        command_response_body: list | dict | None = None
        try:
            conn = psycopg2.connect(conn_str)
            with conn.cursor() as cursor:
                command_response_body = handler(conn, cursor)
                if command_response_body is None:
                    if cursor.rowcount >= 0:
                        command_response_body = {"result": f"{cursor.rowcount} rows affected"}
                    else:
                        command_response_body = {"result": "ok"}
        except Exception as e:
            error = {"error_code": e.__class__.__name__, "message": f"Error executing sql statement: {str(e)}"}
        finally:
            if conn is not None:
                conn.close()

        command_response: CommandResponseDict = {
            "body": command_response_body,
            "mimetype": "application/json",
        }
        return_response: ConnectorProxyResponseDict = {
            "command_response": command_response,
            "error": error,
            "command_response_version": 2,
        }

        return return_response


    def execute_query(self, sql: str, conn_str: str, values: list | None=None) -> ConnectorProxyResponseDict:
        def handler(conn: Any, cursor: Any) -> None:
            cursor.execute(sql, values)
            conn.commit()

        return self._execute(sql, conn_str, handler)

    def execute_batch(self, sql: str, conn_str: str, vars_list: list) -> ConnectorProxyResponseDict:
        def handler(conn: Any, cursor: Any) -> None:
            cursor.executemany(sql, vars_list)
            # TODO: look more into getting this to work instead
            # psycopg2.extras.execute_batch(cursor, sql, vars_list)
            # https://www.psycopg.org/docs/extras.html#fast-exec
            conn.commit()

        return self._execute(sql, conn_str, handler)

    def fetchall(self, sql: str, conn_str: str, values: list) -> ConnectorProxyResponseDict:
        def prep_results(results: list) -> list:
            return [r[0][1:-1].replace('"', '').split(",") for r in results]
        def handler(conn: Any, cursor: Any) -> list:
            cursor.execute(sql, values)
            return prep_results(cursor.fetchall())

        return self._execute(sql, conn_str, handler)

    def build_where_clause(self, schema: dict[str, Any]) -> tuple[str, Any]:
        where_configs = schema.get("where", [])
        if len(where_configs) == 0:
            return "", None

        operators = {"=", "!=", "<", ">"}

        def build_where_part(where_config: list) -> tuple[str, Any]:
            column, operator, value = where_config
            if operator not in operators:
                raise Exception(f"Unsupported operator '{operator}' in where clause")
            return (f"{column} {operator} %s", value)

        where_parts = map(build_where_part, where_configs)
        columns, values = zip(*where_parts, strict=True)

        return f"WHERE {' AND '.join(columns)}", values
