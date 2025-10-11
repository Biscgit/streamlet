import re
from importlib import metadata
from unittest.mock import mock_open, patch

import pytest
import yaml

from core import Settings, validation
from core.metric import MetricFrame
from tests.fixtures.e2e import run_streamlet


class FakePath:
    schema: str

    def __call__(self, v):
        return v


@pytest.mark.parametrize("streamlet", [tuple()], indirect=True)
class TestFlow:
    """Tests flow functionalities."""

    def test_startup_summary(self, monkeypatch, streamlet):
        config = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "debug",
                    "tasks": [
                        {
                            "name": "test_task1",
                            "cron": "0 * * * *",
                            "params": {"payload": {"metric": 1}},
                        },
                        {
                            "name": "test_task2",
                            "cron": "3 * * * *",
                            "params": {"payload": {"metric": 2}},
                        },
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        Settings.hide_welcome = False
        stdout = []

        def fake_print(t, *_, **__):
            stdout.append(t)

        with patch("core.flow.print", fake_print):
            flow = streamlet(config)
            run_streamlet(flow)

        text = "\n".join(stdout)

        # check version
        version = re.findall(r"^Version:.*", text, re.MULTILINE)[0]
        assert metadata.version("streamlet") in version

        # check task numbers
        tasks = re.findall(r"^ {2}\[in .*].*:", text, re.MULTILINE)
        assert len(tasks) == 2

        # check names
        name_1 = re.match(r"^ {2}\[.*] (.*) :.*", tasks[0]).groups()[0]
        name_2 = re.match(r"^ {2}\[.*] (.*) :.*", tasks[1]).groups()[0]
        assert name_1 == "test_task1"
        assert name_2 == "test_task2"

        # check cron minutes
        minute_1 = re.match(r"^ {2}\[in 00:(.*):.*].*", tasks[0]).groups()[0]
        minute_2 = re.match(r"^ {2}\[in 00:(.*):.*].*", tasks[1]).groups()[0]
        assert int(minute_1) % 60 == (int(minute_2) - 3) % 60

    def test_extend_config(self, monkeypatch, streamlet):
        monkeypatch.setattr(validation.schemas, "PathExists", FakePath)

        config = {
            "flow": {"version": "v1", "extends": ["some/path.yaml"]},
            "input": [
                {
                    "type": "debug",
                    "name": "abc",
                    "tasks": [
                        {"name": "test_payload1", "cron": "* * * * *", "params": {"payload": {}}},
                        {"name": "test_payload2", "cron": "* * * * *", "params": {"payload": {}}},
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }
        extend = {
            "input": [
                {
                    "type": "debug",
                    "name": "abc",
                    "tasks": [
                        {"name": "test_payload1", "cron": "*/2", "params": {"payload": {"a": 1}}},
                        {"name": "test_payload3", "cron": "0 0 * * *", "params": {"payload": {}}},
                    ],
                }
            ]
        }
        content = yaml.dump(extend)

        with patch("core.flow.open", mock_open(read_data=content)):
            flow = streamlet(config)

        task_confs = flow.inputs[0].task_confs
        assert len(task_confs) == 3

        assert task_confs[0]["name"] == "test_payload1"
        assert task_confs[0]["cron"] == "*/2"

        assert task_confs[1]["name"] == "test_payload2"
        assert task_confs[1]["cron"] == "* * * * *"

        assert task_confs[2]["name"] == "test_payload3"
        assert task_confs[2]["cron"] == "0 0 * * *"

    def test_metric_patterns(self, monkeypatch, streamlet):
        # have `fieldg` match two patterns
        payload = {"field": 1, "field_a": 2, "gg": 4, "sfield": 3, "fieldg": 5}
        config = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "debug",
                    "tasks": [
                        {
                            "name": "test_task1",
                            "result": {"metrics": ["field*", "*g"]},
                            "cron": "0 * * * *",
                            "params": {"payload": payload},
                        },
                    ],
                }
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        Settings.hide_welcome = False

        flow = streamlet(config)
        run_streamlet(flow)

        frame: "MetricFrame" = flow.debug_output[0]
        frame.sort(key=lambda x: x.metric_field_name)

        assert len(frame) == 4

        assert frame[0].attributes == {"sfield": 3}
        assert frame[0].metric == 1
        assert frame[0].metric_field_name == "field"

        assert frame[1].attributes == {"sfield": 3}
        assert frame[1].metric == 2
        assert frame[1].metric_field_name == "field_a"

        assert frame[2].attributes == {"sfield": 3}
        assert frame[2].metric == 5
        assert frame[2].metric_field_name == "fieldg"

        assert frame[3].attributes == {"sfield": 3}
        assert frame[3].metric == 4
        assert frame[3].metric_field_name == "gg"
