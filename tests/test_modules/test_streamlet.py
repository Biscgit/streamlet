from time import sleep

import pytest

from core.metric import MetricFrame
from tests.fixtures.e2e import run_streamlet

# Set these to the supported versions of https://dbod.web.cern.ch
STREAMLET_MODULES = ["src.modules.streamlet"]


def get_result(frame: MetricFrame, **filters):
    return [m for m in frame if all(m.get(k) == v for k, v in filters.items())]


@pytest.mark.parametrize("streamlet", [STREAMLET_MODULES], indirect=True)
class TestModuleStreamlet:
    def test_shutdown_hook(self, streamlet):
        configuration = {
            "flow": {"version": "v1"},
            "input": [
                {
                    "type": "streamletmetrics",
                    "tasks": [{"name": "collect_logs", "cron": "*"}],
                },
                {
                    "type": "debug",
                    "tasks": [{"name": "no_data", "cron": "*", "params": {"payload": []}}],
                },
            ],
            "output": [{"type": "debug", "name": "debug"}],
        }

        flow = streamlet(configuration)
        run_streamlet(flow)
        sleep(1)

        assert len(flow.debug_output) == 2
        frame: MetricFrame = flow.debug_output[1]

        task_count_ok = get_result(
            frame, streamlet_metric_type="task_result", streamlet_result="okay"
        )[0]
        assert task_count_ok.metric == 1

        task_count_skipped = get_result(
            frame, streamlet_metric_type="task_result", streamlet_result="skipped"
        )[0]
        assert task_count_skipped.metric in [0, 1]

        task_count_failed = get_result(
            frame, streamlet_metric_type="task_result", streamlet_result="failed"
        )[0]
        assert task_count_failed.metric == 0
