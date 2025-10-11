"""This module contains the StreamletTask class, responsible for creating and scheduling tasks."""

import enum
import fnmatch
import operator
import time
import traceback
import typing
from collections import deque
from datetime import datetime
from functools import reduce
from zoneinfo import ZoneInfo

from celery import Task
from celery.exceptions import TaskPredicate

from core.helpers import flatten, parse_cron
from core.logger import get_logger
from core.metric import Metric, MetricFrame
from core.settings import Settings

if typing.TYPE_CHECKING:
    from core.abstract import AbstractInput, AbstractModule


class States(enum.IntEnum):
    """Task finishing states."""

    FINISHED = 0
    SKIPPED = 1


# pylint: disable=R0903
class StreamletTask:
    """Acts as a Metaclass for the Blueprint needed to configure Celery Tasks."""

    ALL_CLS: dict[str, type["StreamletTaskBlueprint"]] = {}
    LAST_TASK_IDS: deque[tuple[datetime, str]] = None

    def __new__(cls, input_module: "AbstractInput", config: dict):
        name = f"{Settings.task_name_prefix}{config["name"]}"
        cls_name = f"StreamletTask_{name}"
        meta = type(
            cls_name,
            (StreamletTaskBlueprint,),
            {
                "name": name,
                "max_retries": config["max_retries"],
                "default_retry_delay": config["retry_delay"],
            },
        )

        if StreamletTask.ALL_CLS.get(cls_name):
            logger = get_logger("flow")
            logger.error("Task with name `%s` already exists!", name)
            raise ValueError("Duplicated task names.")

        if StreamletTask.LAST_TASK_IDS is None:
            StreamletTask.LAST_TASK_IDS = deque(maxlen=Settings.task_id_queue_size)

        StreamletTask.ALL_CLS[cls_name] = meta
        return meta(input_module, config)

    # @staticmethod
    # def log_information(log: str):
    #     """Extracts task information from provided log"""
    #     pattern = r"\[(?P<timestamp>.*): (?P<level>\w+)\/task] \|\>(?P<task>.*)\> (?P<message>.+)"
    #     res = re.match(pattern, log)
    #     return res.groupdict() if res else {}


class StreamletTaskBlueprint(Task):  # , metaclass=StreamletTaskMeta):
    """Base task to be executed by Celery."""

    ignore_result = False
    task_logger = None

    def __init__(self, input_module: "AbstractInput", config: dict):
        # references
        self.input = input_module
        self.flow = input_module.flow
        self.config = config

        # configure task
        self.enabled = config["enabled"] and input_module.enabled
        self.logger.debug("Creating Task%s.", "" if self.enabled else " (disabled)")

        # add other variables
        self.schedule = parse_cron(config["cron"])
        self.celery_task = None

    def register(self):
        """Register the task to celery application."""
        self.celery_task = self.flow.register_task(self, bind=True)

        if self.enabled:
            self.logger.debug("Registering task.")

            if Settings.run_once:
                self.delay()
            else:
                self.flow.add_periodic_task(sig=self.s(), name=self.name, schedule=self.schedule)
        else:
            self.logger.info("Disabled. Skipping registration.")

    @property
    def logger(self):
        """Get module specific logger"""
        cls = self.__class__
        if cls.task_logger is None:
            logger = get_logger(f"|>{cls.name}>", "task")
            cls.task_logger = logger

        return cls.task_logger

    def run(self, *args, **kwargs):
        """Wrapper to handle retries and celery states."""

        t = time.perf_counter()
        try:
            code = self.streamlet_exec(self.request.metadata)

        except TaskPredicate as e:
            raise e

        except Exception as e:
            raise self.retry(throw=False, exc=e)

        msg = "skipped after" if code == States.SKIPPED else "finished in"
        self.logger.info("Task %s %0.3fs", msg, time.perf_counter() - t)

        return code

    def streamlet_exec(self, task_data: dict):
        """Executes the task's logic."""

        self.logger.debug("Generated empty Dataframe.")
        static_data = MetricFrame(self.name)

        task_data["last_module"] = self.input.name
        self.input.modify(static_data)

        self.logger.debug(
            "Fetching from Input %s (%s)", self.input.name, self.input.__class__.__name__
        )
        result = self.input(self.config["params"].copy())
        self.process_result(static_data, result)

        # clear up memory
        del result

        if len(static_data) == 0:
            return States.SKIPPED

        filter_ = {"input_name": self.input.name, "task_name": self.name}

        for mod in [m for m in self.flow.transforms if m.accepts_from(**filter_)]:
            self.logger.debug(
                "Passing Dataframe[s=%d] to Transform %s (%s).",
                *(len(static_data), mod.name, mod.__class__.__name__),
            )
            task_data["last_module"] = mod.name
            mod(static_data)

        self.logger.debug("Freezing Dataframe[s=%d].", len(static_data))
        static_data.freeze()

        for mod in [m for m in self.flow.outputs if m.accepts_from(**filter_)]:
            self.logger.debug(
                "Passing Dataframe[s=%d] to Output %s (%s).",
                *(len(static_data), mod.name, mod.__class__.__name__),
            )
            task_data["last_module"] = mod.name
            finished_data = static_data.copy()

            mod.modify(finished_data)
            mod(finished_data)

        return States.FINISHED

    def process_result(self, data: MetricFrame, result: list[dict] | dict) -> None:
        """Process results from an input."""
        self.logger.debug("Processing input results...")
        res_conf = self.config["result"]
        sep = Settings.nested_attr_seperator

        queue = deque()
        result = result if isinstance(result, list) else [result]

        for frame in result:
            # extract metrics from frame
            metrics = dict(self.extract_metrics(frame, sep, res_conf["metrics"]))

            if (attr := res_conf["attributes"]) is not None:
                frame = {k: v for k, v in frame.items() if k in attr}

            # try creating empty metrics if only attributes set/found
            metrics = metrics or {None: None}

            # append all metrics
            for metric_name, metric in metrics.items():
                attributes = frame.copy() | self.config["static_attributes"]
                queue.append(Metric(data, attributes, metric, metric_name))

        # clean up unwanted entries from data
        data[:] = list(queue)

    @staticmethod
    def extract_metrics(obj: dict, sep: str, fields):
        """Extracts (nested) metric fields from raw data."""
        keys = {sep.join([str(f) for f in f]): tuple(f) for f in flatten(obj).keys()}

        for pattern in fields or []:
            for key in fnmatch.filter(list(keys.keys()), pattern):
                *chain, top = key.split(sep)

                nested: dict = reduce(operator.getitem, chain, obj)
                yield key, nested.pop(top)

                del keys[key]

    def before_start(self, task_id, args, kwargs):
        if not self.request.retries:
            StreamletTask.LAST_TASK_IDS.append((datetime.now(ZoneInfo(Settings.timezone)), task_id))
            self.logger.info("Running Task with id %s.", task_id)

        self.request.start_time = time.perf_counter()
        self.request.metadata = {"last_module": "<None>"}

    def on_success(self, retval, task_id, args, kwargs):  # pylint: disable=R0913,R0917
        if retval not in States:
            self.logger.error("Returned unknown code `%s`", retval)
            raise ValueError("Unknown task return code")

    def on_retry(self, exc, task_id, args, kwargs, einfo):  # pylint: disable=R0913,R0917
        self.logger.warning(
            "[%s > %s] in %s after %0.3fs. Retrying %d/%d times.",
            exc.__class__.__module__,
            exc.__class__.__name__,
            self.request.metadata["last_module"],
            time.perf_counter() - self.request.start_time,
            self.request.retries + 1,
            self.max_retries,
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):  # pylint: disable=R0913,R0917
        self.logger.error(
            "[%s > %s] in %s after %0.3fs. Exhausted retries, terminating Task.",
            exc.__class__.__module__,
            exc.__class__.__name__,
            self.request.metadata["last_module"],
            time.perf_counter() - self.request.start_time,
        )

        if Settings.print_traceback:
            traceback.print_tb(exc.__traceback__)

    def get_chain(self, include_input=True, ignore_enabled=False) -> list["AbstractModule"]:
        """Calculates and prints the whole path of the task."""
        inp, name = self.input.name, self.name

        modules = self.flow.transforms + self.flow.outputs
        chain = [m for m in modules if m.accepts_from(inp, name, ignore_enabled)]

        return ([self.input] if include_input else []) + chain
