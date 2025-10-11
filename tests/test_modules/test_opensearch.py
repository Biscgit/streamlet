import copy
import logging
from datetime import datetime, timedelta, timezone

import pytest
from testcontainers.opensearch import OpenSearchContainer

from core.metric import MetricFrame
from tests import FULL_TEST
from tests.fixtures.e2e import run_streamlet

OPENSEARCH_VERSIONS = ["2", "3"] if FULL_TEST else ["2"]
STREAMLET_MODULES = ["src.modules.opensearch"]


@pytest.fixture(params=OPENSEARCH_VERSIONS, scope="class")
def database(request):
    image = f"opensearchproject/opensearch:{request.param}"
    with OpenSearchContainer(image, security_enabled=False) as db:
        logging.info("Using storage OpenSearch: %s", db.image)
        yield db


@pytest.fixture(scope="function")
def populated_database(database):
    logging.info("Populating Database...")

    index_name = "testing_data"

    client = database.get_client()
    client.indices.create(index=index_name)
    client.indices.create(index="testing_results")

    timestamp = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    for i in range(120):
        doc = {
            "timestamp": (timestamp - timedelta(minutes=(i + 10))).isoformat(),
            "metric": i,
            "field_1": ["a", "b", "c", "d", "e", "f", "g"][i % 7],
            "field_2": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][i % 11],
            "metadata": {"host": f"pod_{i % 3}", "environment": "testing"},
        }
        client.index(index=index_name, body=doc, id=f"doc_{i + 1}")

    yield database

    logging.info("Dropping old indices")
    client.indices.delete(index=index_name)
    client.indices.delete(index="testing_results")


def base_input_config(connection):
    return {
        "flow": {"version": "v1"},
        "input": [
            {
                "type": "opensearch",
                "connection": {
                    "host": connection["host"],
                    "port": connection["port"],
                    "auth": {
                        "username": connection["username"],
                        "password": connection["password"],
                    },
                    "cert_path": None,
                    "use_ssl": False,
                },
                "tasks": [],
            }
        ],
        "output": [{"type": "debug", "name": "debug"}],
    }


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleOpenSearch:
    def test_read_dev_tools(self, streamlet, populated_database):
        connection = populated_database.get_config()
        task_config = {
            "name": "example_query",
            "cron": "* * * * *",
            "result": {"metrics": "_source.metric"},
            "params": {
                "size": 999,
                "result": "documents",
                "index": "testing_data",
                "queries": {"term": {"metadata.host": {"value": "pod_1"}}},
            },
        }

        configuration = base_input_config(connection)
        configuration["input"][0]["tasks"].append(task_config)

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]

        expected_metrics = [x + 1 for x in range(120) if x % 3 == 0]
        difference = [x - y for x, y in zip([m.metric for m in frame], expected_metrics)]
        assert all(d == difference[0] for d in difference)

    def test_read_lucene(self, streamlet, populated_database):
        connection = populated_database.get_config()
        task_config = {
            "name": "example_query",
            "cron": "* * * * *",
            "result": {"metrics": "_source.metric"},
            "params": {
                "size": 999,
                "result": "documents",
                "index": "testing_data",
                "lucene": "metadata.host: pod_1",
            },
        }

        configuration = base_input_config(connection)
        configuration["input"][0]["tasks"].append(task_config)

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]

        expected_metrics = [x + 1 for x in range(120) if x % 3 == 0]
        assert [m.metric for m in frame] == expected_metrics

    def test_read_aggs(self, streamlet, populated_database):
        connection = populated_database.get_config()
        task_config = {
            "name": "example_query",
            "cron": "* * * * *",
            "result": {"metrics": ["total", "doc_count"]},
            "params": {
                "index": "testing_data",
                "aggs": {
                    "title": {
                        "terms": {"field": "field_1.keyword"},
                        "aggs": {"total": {"sum": {"field": "metric"}}},
                    }
                },
            },
        }

        configuration = base_input_config(connection)
        configuration["input"][0]["tasks"].append(task_config)

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]

        assert len(frame) == 2 * 7

        expected = [17, 17, 17, 17, 17, 17, 18]
        assert sum(expected) == 120
        assert sorted([m.metric for m in frame if m.metric_field_name == "doc_count"]) == expected

        expected = [969 + 17 * i for i in range(7)]
        assert sum(expected) == sum(i for i in range(120))
        assert sorted([m.metric for m in frame if m.metric_field_name == "total"]) == expected

    def test_read_timeframe(self, streamlet, populated_database):
        connection = populated_database.get_config()
        task_config = {
            "name": "example_query",
            "cron": "* * * * *",
            "result": {"metrics": "_source.metric"},
            "params": {
                "size": 999,
                "result": "documents",
                "index": "testing_data",
                "timerange": {
                    "from": "now/h-30m",
                    "to": "now/h-10m",
                    "timestamp_field": "timestamp",
                },
            },
        }

        configuration = base_input_config(connection)
        configuration["input"][0]["tasks"].append(task_config)

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert len(frame) == 20

    def test_index_resolve(self, streamlet, populated_database):
        connection = base_input_config(populated_database.get_config())["input"][0]["connection"]
        payload = [{"metric": i, "index": f"doc_{i + 1}", "env": "test"} for i in range(20)]
        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "debug",
                    "tasks": [
                        {
                            "name": "test_payload",
                            "cron": "* * * * *",
                            "params": {"payload": payload},
                        }
                    ],
                }
            ],
            "transform": [
                {
                    "type": "OpenSearchIndexMapping",
                    "connection": connection,
                    "params": {
                        "search_index": "testing_data",
                        "search_index_key": "index",
                        "include_attributes": ["field_2"],
                        "fail_on_not_found": True,
                    },
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert len(frame) == 20

        keys = ["env", "field_2", "index"]
        assert all(sorted(m.attributes.keys()) == keys for m in frame)
        assert all(m.metric == i for i, m in enumerate(frame))

        expected = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][i % 11] for i in range(len(frame))]
        assert [m["field_2"] for m in frame] == expected

    def test_write(self, streamlet, populated_database):
        connection = base_input_config(populated_database.get_config())["input"][0]["connection"]
        payload = [{"metric": i, "index": f"doc_{i}", "env": "test"} for i in range(20)]

        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "debug",
                    "tasks": [
                        {
                            "name": "test_payload",
                            "cron": "* * * * *",
                            "params": {"payload": copy.deepcopy(payload)},
                        }
                    ],
                }
            ],
            "output": [
                {
                    "type": "opensearch",
                    "connection": connection,
                    "params": {"index": "testing_results"},
                }
            ],
        }

        flow = streamlet(configuration)
        run_streamlet(flow)

        client = populated_database.get_client()
        request = client.search(index="testing_results", body={"size": 500})
        inserted_entries = [r["_source"] for r in request["hits"]["hits"]]

        assert len(inserted_entries) == len(payload)
        assert sorted(inserted_entries, key=lambda x: x["metric"]) == payload
