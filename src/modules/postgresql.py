"""This file contains modules for PostgreSQL requests."""

import simplejson as json
from psycopg.rows import dict_row
from psycopg_pool.pool import ConnectionPool
from voluptuous import Clamp, Maybe, Optional, Switch

from core.abstract import AbstractInput
from core.modules import Importable
from core.validation import EnvironmentVar, TimeToSeconds


@Importable
class PostgreSQL(AbstractInput):
    """An input module that retrieves data from a PostgreSQL database."""

    def __init__(self, *args):
        super().__init__(*args)

        if uri := self.connection_config.get("uri"):
            try:
                conn = [f"postgresql://{uri.split("://", maxsplit=1)[1]}"]
            except IndexError:
                conn = [uri]
        else:
            conf = self.connection_config
            conn = {"dbname": conf["dbname"], "port": conf["port"], "host": conf["host"]}
            if auth := conf["auth"]:
                conn |= {"user": auth["username"], "password": auth["password"]}

            conn = [f"{k}={v}" for k, v in conn.items()]

        self._connection: ConnectionPool = ConnectionPool(
            " ".join(conn),
            min_size=self.connection_config["min_size"],
            max_size=self.connection_config["max_size"],
            open=False,
        )

    def on_connect(self) -> None:
        self.logger.info("Connecting to PostgreSQL database.")
        self._connection.open(wait=True, timeout=10)

    def on_shutdown(self) -> None:
        if not self._connection.closed:
            self._connection.close(timeout=10)
        self.logger.info("Closed all PostgreSQL connections")

    @classmethod
    def connection_schema(cls) -> Switch:
        """
        :uri: Database URI to connect to
        :dbname: Database name
        :auth: Basic auth with username and password
        :host: Host url
        :port: Database port
        :min_size: Minimum Connection Pool size
        :max_size: Maximum Connection Pool size
        """
        size_schema = {
            Optional("min_size", default=1): Clamp(min=1),
            Optional("max_size", default=8): Clamp(max=256),
        }
        auth = {"username": Maybe(EnvironmentVar()), "password": Maybe(EnvironmentVar())}

        return Switch(
            {
                "dbname": EnvironmentVar(),
                "host": EnvironmentVar(),
                "port": EnvironmentVar(int),
                Optional("auth", default={}): auth,
                **size_schema,
            },
            {
                "uri": EnvironmentVar(),
                **size_schema,
            },
        )

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :query: SQL query for fetching data
        :timeout: Request timeout
        """
        return {
            "query": str,
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
        }

    def __call__(self, params: dict):
        with self._connection.connection(timeout=10) as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(f"SET statement_timeout = {params["timeout"] * 1000}")
                cursor.execute(params["query"])

                rows = cursor.fetchall()
                json_results = json.loads(json.dumps(rows, use_decimal=True, default=str))

        return json_results
