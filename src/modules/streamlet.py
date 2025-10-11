"""This file contains modules for Streamlet metrics."""

import logging
import random
import sys
import threading
import typing
from datetime import datetime
from zoneinfo import ZoneInfo

import simplejson as json
from voluptuous import All, Any, Coerce, Lower, Maybe, Optional, Upper, ValueInvalid

from core import Settings
from core.abstract import AbstractInput, AbstractOutput
from core.modules import Importable
from core.task import States, StreamletTask

if typing.TYPE_CHECKING:
    from celery.result import AsyncResult

    from core.metric import MetricFrame


class MetricHook(logging.Handler):
    """Custom log handler for metrics"""

    def __init__(self, parent: "StreamletMetrics", task_conf: dict):
        super().__init__()
        self.setLevel(logging.DEBUG)
        self._parent = parent
        self._level: int = logging.getLevelName(task_conf["params"]["level"])

        self.storage = {n: 0 for n in sorted(logging._levelToName.keys()) if n >= self._level}
        self.storage_lock = threading.RLock()

    def emit(self, record: logging.LogRecord):
        level = record.levelno
        if level >= self._level:
            with self.storage_lock:
                self.storage[level] += 1

    def reset_storage(self):
        """Reset local storage."""
        self.storage = {k: 0 for k in self.storage.keys()}


@Importable()
class StreamletMetrics(AbstractInput):
    """Integral module of collecting errors from streamlet."""

    def __init__(self, *args):
        super().__init__(*args)
        if len(self.task_confs) != 1:
            raise ValueInvalid(
                f"StreamletMetrics must have only one single task, not {len(self.task_confs)}.",
                path=[self.name],
            )

        self.run_timestamp = datetime.now(ZoneInfo(Settings.timezone))
        self.hook = MetricHook(self, self.task_confs[0])
        self.pending = []

    def on_connect(self) -> None:
        # pylint: disable=E1101
        for logger in logging.root.manager.loggerDict.values():
            if isinstance(logger, logging.Logger):
                logger.addHandler(self.hook)

        self.logger.info("Streamlet will collect logging and task execution information.")

    def on_pre_shutdown(self):
        self.logger.info("Flushing Streamlet metrics...")
        self.tasks[0].celery_task.apply()

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :level: Log level to filter
        """
        log_levels = list(getattr(logging, "_nameToLevel").keys())
        return {Optional("level", default="DEBUG"): All(Coerce(Upper), Any(*log_levels))}

    def __call__(self, params: dict) -> list[dict]:
        current = datetime.now(ZoneInfo(Settings.timezone))

        task_mapping = StreamletTask.LAST_TASK_IDS
        task_ids = [t for d, t in task_mapping if d >= self.run_timestamp] + self.pending

        self.logger.debug("Fetching %d results from celery backend...", len(task_ids))
        results: list["AsyncResult"] = [self.flow.AsyncResult(_id) for _id in task_ids]

        self.pending = [r.task_id for r in results if r.state == "PENDING"]
        results = [r for r in results if r not in self.pending]

        base = {
            "timestamp_from": self.run_timestamp.isoformat(),
            "timestamp_to": current.isoformat(),
        }
        dataset = [
            {
                "metric": sum(1 for r in results if r.result == States.FINISHED),
                "streamlet_metric_type": "task_result",
                "streamlet_result": "okay",
            },
            {
                "metric": sum(1 for r in results if r.result == States.SKIPPED),
                "streamlet_metric_type": "task_result",
                "streamlet_result": "skipped",
            },
            {
                "metric": sum(1 for r in results if r.state == "FAILURE"),
                "streamlet_metric_type": "task_result",
                "streamlet_result": "failed",
            },
        ]

        with self.hook.storage_lock:
            dataset.extend(
                [
                    {"metric": count, "streamlet_metric_type": "log", "streamlet_log_level": level}
                    for level, count in self.hook.storage.items()
                ]
            )
            self.hook.reset_storage()

        self.run_timestamp = current
        return [r | base for r in dataset]


@Importable()
class ConsoleOutput(AbstractOutput):
    """Output that prints all data it receives to the console."""

    def __init__(self, *args):
        super().__init__(*args)
        self._pipe = {"stdout": sys.stdout, "stderr": sys.stderr}[self.settings["pipe"]]
        self._width = self.settings["width"]

    @classmethod
    def params_schema(cls) -> dict:
        """
        :pipe: std pipe to write output to
        :width: terminal width
        """
        return {
            Optional("pipe", default="stdout"): All(Lower, Any("stdout", "stderr")),
            Optional("width", default=None): Maybe(int),
        }

    def __call__(self, data: "MetricFrame"):
        for metric in data:
            out = json.dumps(dict(metric), ensure_ascii=False)
            if self._width is not None:
                out = out[: self._width - 3] + "..." if len(out) > self._width else out

            self._pipe.write(f"{out}\n")
            self._pipe.flush()


@Importable()
class RandomMetrics(AbstractInput):
    """Sends random numbers as metrics."""

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :min: Lower bound
        :max: Upper bound
        :count: Number of metrics returned
        :attributes: Additional attributes
        """
        return {
            Optional("min", default=0): int,
            Optional("max", default=100): int,
            Optional("count", default=1): int,
            Optional("attributes", default={}): {str: object},
        }

    def __call__(self, params: dict) -> list[dict]:
        values = []
        frame = abs(params["max"] - params["min"])

        for _ in range(params["count"]):
            number = random.random() * frame + params["min"]
            values.append({"metric": number, **params["attributes"]})

        return values
