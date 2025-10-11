from datetime import datetime, timezone

import pytest
import simplejson as json
from pytest_httpserver import httpserver as http

from tests.fixtures.e2e import run_streamlet

STREAMLET_MODULES = ["src.modules.monit"]


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleMONIT:
    def test_sending_data(self, streamlet, httpserver: http.HTTPServer):
        data = [
            {"metric": 66, "attr": "series1"},
            {"metric": 34, "attr": "series2"},
            {"metric": 28, "attr": "series2"},
        ]
        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "debug",
                    "tasks": [{"name": "task_name", "cron": "*", "params": {"payload": data}}],
                },
            ],
            "output": [
                {"type": "debug", "name": "debug"},
                {
                    "type": "monitmetric",
                    "modifiers": {"time_modulus": "1d"},
                    "connection": {
                        "auth": {"username": "test_user", "password": "******"},
                        "endpoint": f"http://{httpserver.host}:{httpserver.port}",
                    },
                    "params": {"environment": "dev", "monit_type_prefix": "snw"},
                },
            ],
        }

        cur_time = datetime.now(timezone.utc).replace(microsecond=0, second=0, minute=0, hour=0)
        expected = [
            {
                "producer": "test_user",
                "type": "task_name",
                "type_prefix": "snw",
                "environment": "dev",
                "timestamp": cur_time.timestamp(),
                "data": d,
            }
            for d in data
        ]

        httpserver.expect_request(
            "/test_user",
            method="POST",
            data=json.dumps(expected, sort_keys=True),
        ).respond_with_json({"status": "ok"})

        flow = streamlet(configuration)
        run_streamlet(flow)

        assert len(flow.debug_output) == 1
        assert len(flow.debug_output[0]) == 3
