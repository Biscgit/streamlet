import pytest
from pytest_httpserver import httpserver as http

from core import Settings
from core.metric import MetricFrame
from tests.fixtures.e2e import run_streamlet

STREAMLET_MODULES = ["src.modules.http"]


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleHTTP:
    def test_http_raw_fetch(self, streamlet, httpserver: http.HTTPServer):
        Settings.allow_none_metric = True

        message = "This is a message!"
        httpserver.expect_request("/api/some_text").respond_with_data(message)

        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "rawhttp",
                    "connection": {"endpoint": f"http://{httpserver.host}:{httpserver.port}"},
                    "tasks": [
                        {
                            "name": "test_request",
                            "cron": "*",
                            "params": {"path": "/api/some_text", "response_field": "message_field"},
                        }
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert len(frame) == 1

        metric = frame[0]
        assert metric.metric is None
        assert metric.attributes == {"message_field": message}

    def test_http_json_fetch(self, streamlet, httpserver: http.HTTPServer):
        message = [{"name": {"letter": t}, "value": i} for t, i in [("a", 1), ("b", 2), ("c", 3)]]
        httpserver.expect_request("/json").respond_with_json(message)

        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "jsonhttp",
                    "connection": {"endpoint": f"http://{httpserver.host}:{httpserver.port}"},
                    "tasks": [
                        {
                            "result": {"metrics": "value"},
                            "name": "test_request",
                            "cron": "* * * * *",
                            "params": {"path": "/json"},
                        }
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert len(frame) == 3

        assert sorted([m.metric for m in frame]) == [1, 2, 3]
        assert sorted([m["name.letter"] for m in frame]) == ["a", "b", "c"]
