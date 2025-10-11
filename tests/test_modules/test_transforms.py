import logging

import pytest

from core import Settings
from core.metric import MetricFrame
from tests.fixtures.e2e import run_streamlet

STREAMLET_MODULES = ["src.modules.transforms"]


@pytest.fixture()
def payload():
    return [{"name": {"letter": t}, "metric": i} for t, i in [("a", 1), ("b", 2), ("c", 3)]]


def config_payload(payload):
    task = {"name": "test_payload", "cron": "* * * * *", "params": {"payload": payload}}
    return {
        "flow": {"version": "v1"},
        "input": [{"type": "debug", "tasks": [task]}],
        "output": [{"type": "debug", "name": "debug"}],
    }


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleCodeTransform:

    @pytest.mark.parametrize("check_logs", [(logging.CRITICAL,)], indirect=True)
    def test_disabled_settings(self, streamlet, check_logs):
        Settings.allow_exec = False

        configuration = config_payload([])
        configuration["transform"] = [
            {
                "type": "codetransform",
                "params": {"src": "None", "mode": "metric"},
            }
        ]

        with pytest.raises(ValueError):
            streamlet(configuration)

    @pytest.mark.parametrize("check_logs", [(logging.CRITICAL,)], indirect=True)
    def test_code_syntax_error(self, streamlet, check_logs):
        Settings.allow_exec = True

        configuration = config_payload([])
        configuration["transform"] = [
            {
                "type": "codetransform",
                "params": {"src": "data['field'] = [1, 2; 3]", "mode": "metric"},
            }
        ]

        with pytest.raises(SyntaxError):
            streamlet(configuration)

    def test_run_code(self, streamlet, payload):
        Settings.allow_exec = True

        configuration = config_payload(payload)
        configuration["transform"] = [
            {
                "type": "codetransform",
                "params": {
                    "builtins": "int",
                    "src": "print(data)\ndata['field']={}\ndata['field.nested'] = int('123')",
                    "mode": "metric",
                },
            }
        ]

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert sorted([m.metric for m in frame]) == [1, 2, 3]
        assert all(m["field"]["nested"] == 123 for m in frame)
        assert all(m["field.nested"] == 123 for m in frame)


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleKeyMapper:
    def test_mapping(self, streamlet, payload):
        configuration = config_payload(payload)
        configuration["transform"] = [
            {
                "type": "keymapping",
                "params": {
                    "mapping": {1: "hello", 2: "world"},
                    "key": "metric",
                    "default": "unknown",
                },
            }
        ]

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert [m.metric for m in frame] == ["hello", "world", "unknown"]
        assert [m["name.letter"] for m in frame] == ["a", "b", "c"]

    @pytest.mark.parametrize("check_logs", [(logging.CRITICAL,)], indirect=True)
    @pytest.mark.parametrize("task_states", [{"passed": 0, "failed": 1}], indirect=True)
    def test_mapping_raise_missing(self, streamlet, payload):
        configuration = config_payload(payload)
        configuration["transform"] = [
            {
                "type": "keymapping",
                "params": {"mapping": {1: "hello", 2: "world"}, "key": "metric"},
            }
        ]

        flow = streamlet(configuration)
        run_streamlet(flow)

        assert flow.debug_output == []


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleSimpleFilter:
    def test_conditions_keep(self, streamlet, payload):
        configuration = config_payload(payload)
        configuration["transform"] = [
            {
                "type": "simplefilter",
                "params": {
                    "cond": "gt 1",
                    "mode": "keep",
                },
            },
            {
                "type": "simplefilter",
                "params": {
                    "cond": "gt 2",
                    "mode": "drop",
                },
            },
        ]

        flow = streamlet(configuration)
        run_streamlet(flow)

        frame: MetricFrame = flow.debug_output[0]
        assert len(frame) == 1
        assert [m.metric for m in frame] == [2]
