import logging

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from tests import FULL_TEST
from tests.fixtures.e2e import run_streamlet

# Set these to the supported versions of https://dbod.web.cern.ch
POSTGRESQL_VERSIONS = ["14.15", "15.10", "16.6"] if FULL_TEST else ["16.6"]
STREAMLET_MODULES = ["src.modules.postgresql"]


@pytest.fixture(params=POSTGRESQL_VERSIONS)
def database(request):
    with PostgresContainer(f"postgres:{request.param}") as db:
        logging.info("Using storage PostgreSQL: %s", db.image)
        yield db


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModulePostgreSQL:
    # @pytest.mark.parametrize("check_logs", [(30,)])
    def test_fetch_record(self, streamlet, database):
        """Fetch records from a table."""

        conn = {
            "dbname": database.dbname,
            "auth": {"username": database.username, "password": database.username},
            "host": database.get_container_host_ip(),
            "port": database.get_exposed_port(database.port),
        }

        uri = f"postgresql://{conn["auth"]["username"]}:{conn["auth"]["password"]}@{conn["host"]}:{conn["port"]}/{conn["dbname"]}"
        client: psycopg.Connection = psycopg.connect(uri)

        logging.info("Filling DB with values")
        client.execute("CREATE TABLE test(number int, title text)")

        dummy_values = [(i, "title_a" if i % 3 else "title_b") for i in range(20)]
        for pair in dummy_values:
            client.execute("INSERT INTO test VALUES (%s, %s)", pair)

        client.commit()

        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "postgresql",
                    "connection": conn,
                    "tasks": [
                        {
                            "name": "simple_query",
                            "cron": "* * * * *",
                            "params": {
                                "query": "SELECT COUNT(*) as metric, title FROM test GROUP BY title"
                            },
                        }
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        expected = [{"metric": 13, "title": "title_a"}, {"metric": 7, "title": "title_b"}]

        flow = streamlet(configuration)
        run_streamlet(flow)

        dumped = [{m.metric_field_name: m.metric} | m.flatten() for m in flow.debug_output[0]]
        dumped = sorted(dumped, key=lambda x: x["title"])

        assert expected == dumped
