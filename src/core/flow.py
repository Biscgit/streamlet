"""This module is used to build tasks from one configuration file."""

import atexit
import logging
import sys
import threading
import time
import typing
from datetime import datetime, timedelta
from fnmatch import fnmatch
from importlib import metadata
from zoneinfo import ZoneInfo

import simplejson as json
import yaml
from celery import Celery
from celery.apps.beat import Beat
from celery.apps.worker import Worker
from celery.exceptions import OperationalError
from voluptuous.validators import Invalid, IsFile, Schema

from core.abstract import AbstractInput, AbstractModule, AbstractOutput, AbstractTransform
from core.helpers import load_env_pairs, start_readiness_server
from core.logger import get_logger
from core.modules import Modules
from core.settings import Settings
from core.task import StreamletTask
from core.validation import validate
from core.validation.schemas import EnvSchema, FlowSchema, HeaderSchema

logger = get_logger("flow")


class StreamletFlow(Celery):
    """This builds modules and defines tasks to be run."""

    instance = None
    ready = False

    def __init__(self):
        if StreamletFlow.instance:
            raise RuntimeError("Flow has been initialized already!")
        StreamletFlow.instance = self

        self.path = None
        self.configuration = self.get_configuration()

        if env := validate(self.configuration, EnvSchema())["env"]:
            load_env_pairs(env)

        logger.debug("Validating file header...")
        header = validate(self.configuration, HeaderSchema())["flow"]

        Settings.extend(header["settings"])

        if extensions := header["extends"]:
            logger.debug("Found extensions. Loading...")

            try:
                self.load_extensions(extensions)
            except Exception as e:
                logger.error(
                    "Unexpected %s occurred while applying extensions.", e.__class__.__name__
                )
                raise e

        if not (Settings.disable_readiness_probe or Settings.only_validate):
            start_readiness_server(self, Settings.readiness_port)
            logger.info("Started readiness endpoint on port %s.", Settings.readiness_port)

        if env := EnvSchema()(self.configuration)["env"]:
            load_env_pairs(env)
            Settings.initiate()

        logger.info("Loaded configuration from %s", self.path)

        self.configuration = validate(self.configuration, FlowSchema())
        logger.info("Validated Flow configuration.")

        logger.debug("Building inputs...")
        self.inputs: list[AbstractInput] = [
            Modules.create_module(AbstractInput, self, conf, i)
            for i, conf in enumerate(self.configuration["input"])
        ]

        logger.debug("Building transforms...")
        self.transforms: list[AbstractTransform] = [
            Modules.create_module(AbstractTransform, self, conf, i)
            for i, conf in enumerate(self.configuration["transform"])
        ]
        # sort transforms after creation
        self.transforms.sort(key=lambda t: t.priority, reverse=True)

        logger.debug("Building outputs...")
        self.outputs: list[AbstractOutput] = [
            Modules.create_module(AbstractOutput, self, conf, i)
            for i, conf in enumerate(self.configuration["output"])
        ]

        self.check_filters()

        logger.info(
            "Successfully configured %d input(s), %d transform(s) and %d output(s).",
            *(len(self.inputs), len(self.transforms), len(self.outputs)),
        )

        celery_config = {
            "broker_connection_retry_on_startup": True,
            "beat_schedule_filename": Settings.celery_beat_file,
            "result_expires": Settings.celery_result_expire,
            "worker_hijack_root_logger": False,
            "timezone": Settings.timezone,
        }
        super().__init__(
            "Streamlet",
            broker=Settings.celery_broker,
            backend=Settings.celery_backend,
            changes=celery_config,
        )

    def get_configuration(self):
        """Loads and returns the raw configuration."""
        try:
            self.path = Schema(IsFile())(Settings.config)  # pylint: disable=E1120
        except Invalid:
            logger.critical("No configuration file found at %s.", Settings.config)
            sys.exit(1)

        logger.debug("Loading configuration file %s...", self.path)
        with open(self.path, "r", encoding="utf-8") as file:
            try:
                return yaml.safe_load(file)
            except yaml.YAMLError as e:
                logger.error("Invalid YAML file detected: %s", self.path)
                raise e

    def check_broker_connection(self):
        """Check if the broker is available before starting the worker."""
        logger.info("Checking connection to broker...")

        for _ in range(6):
            t = time.time()
            try:
                self.control.ping()
                break
            except OperationalError:
                time.sleep(10 - (time.time() - t))
                logger.debug("Broker is not available yet. Still waiting...")
        else:
            logger.critical("Failed to connect to broker after 60s. Exiting...")
            sys.exit(1)

        logger.info("Broker is available and running.")

    def check_filters(self):
        """Validates the routing of tasks."""
        logger.debug("Validating routes...")

        input_names = [m.name for m in self.inputs]
        task_names = [t.name for t in StreamletTask.ALL_CLS.values()]

        for mod in self.transforms + self.outputs:
            for f in (mod.filters["include_inputs"] or []) + (mod.filters["exclude_inputs"] or []):
                if not any(fnmatch(i, f) for i in input_names):
                    mod.logger.warning("Filter `%s` does not match any Input names.", f)

            for f in (mod.filters["include_tasks"] or []) + (mod.filters["exclude_tasks"] or []):
                if not any(fnmatch(i, f) for i in task_names):
                    mod.logger.warning("Filter `%s` does not match any Task names.", f)

        logger.info("Validated Task routing.")

    def load_extensions(self, extension_paths: list[str]):
        """Loads extensions"""
        f = typing.TypeVar("f")

        def extend(base: f, extension: f):
            if isinstance(extension, list):
                for item in extension:
                    if isinstance(item, dict) and (name := item.get("name")):
                        # ensure correct overwriting of modules and tasks on matching names
                        for i, b in enumerate(base):
                            if name == b.get("name"):
                                base[i] = extend(base[i], item)
                                break
                        else:
                            base.append(item)
                    else:
                        base.append(item)
                return base

            if isinstance(extension, dict):
                for k, v in extension.items():
                    base[k] = extend(base.get(k, {}), v)
                return base

            return extension

        noreq_schema = FlowSchema(required=False)

        logger.debug("Loosely validating configuration for templates...")
        validate(self.configuration, noreq_schema)

        for path in reversed(extension_paths):
            logger.debug("Loading extension %s...", path)

            with open(path, "r", encoding="utf-8") as file:
                try:
                    template = yaml.safe_load(file)
                except yaml.YAMLError as exc:
                    logger.error("Invalid YAML file detected: %s", path)
                    raise exc

            if not template:
                logger.info("Extension %s is empty, skipping.", path)
                continue

            logger.debug("Loosely validating extension...")
            validate(template, noreq_schema)

            self.configuration = extend(self.configuration, template)
            Settings.extend(self.configuration.get("flow", {}).get("settings", {}))

    def on_init(self):
        """This functions adds a task to exit the app after being run once."""
        logger.info("Initiated Celery application.")

        # muting celery info logs
        get_logger("celery").setLevel(Settings.celery_log_level)

        # disable tracebacks
        get_logger("celery.app.trace").propagate = False
        get_logger("celery.app.trace").disabled = True

        # apply additional changes from config
        if Settings.print_config:
            print(json.dumps(self.configuration, indent=2))

        if Settings.only_validate:
            logger.info("Validation of Flow successful.")
            StreamletWorker(app=self).emit_banner()
            sys.exit(0)

        self.check_broker_connection()

        if Settings.run_once:
            logger.info("Running in one-time mode. Exiting after tasks are done.")

            @self.task(name="exit_task")
            def exit_after_done(*_, **__):
                running = -1

                while not 0 < running <= 1:
                    tasks = list(self.control.inspect().active().values())
                    running = sum(len(task) for task in tasks)
                    names = ", ".join([task.get("name", "ERROR") for task in tasks[0][:3]])

                    logger.debug("%d Tasks are running: [%s, ...]", running - 1, names)

                logger.info("All tasks finished. Exiting Streamlet...")
                self.control.broadcast("shutdown")

            exit_after_done.delay()

        logger.debug("Registering tasks from configuration...")

        for t in self.get_tasks():
            t.register()
        logger.info("Registered tasks from configuration.")

    def get_tasks(self):
        """Yields all configured tasks."""
        for i in self.inputs:
            yield from i.tasks

    @classmethod
    def set_ready(cls):
        """Sets flag to be ready and running."""
        cls.ready = True

    def beat(self):
        """Starts beat before worker."""
        logger.debug("Starting celery beat.")

        beat = Beat(app=self, quiet=True)
        threading.Thread(target=beat.run, daemon=True).start()
        return beat


class StreamletWorker(Worker):
    """Custom worker."""

    def __init__(self, app: "StreamletFlow"):
        config = {
            "traceback": Settings.print_traceback,
            "concurrency": Settings.celery_concurrency,
            "pool": Settings.celery_pool,
            "loglevel": logging.getLevelName(Settings.celery_log_level),
        }
        super().__init__(app, **config)

    def emit_banner(self):
        """This function is used to print the startup information."""
        if Settings.hide_welcome:
            return

        flow: StreamletFlow = self.app

        def check(b: bool):
            return "enabled" if b else "disabled"

        def short_id(m: AbstractModule):
            mpg = {AbstractInput: "IN", AbstractTransform: f"TR{m.index}", AbstractOutput: "OUT"}
            for cls, name in mpg.items():
                if issubclass(m.__class__, cls):
                    return f"[{name}]{m.name}"
            raise ValueError(f"Unknown module type: {m.__class__.__name__}")

        pairs: list[tuple[str, str, str]] = []
        for t in sorted(flow.get_tasks(), key=lambda x: x.name):
            td = t.schedule.remaining_estimate(datetime.now(ZoneInfo(Settings.timezone)))
            next_run = td - timedelta(microseconds=td.microseconds)

            chain = " 🢡 ".join([short_id(m) for m in t.get_chain(ignore_enabled=True)])
            delta = f"[in {str(next_run):0>8}]" if t.enabled else "[>disabled!<]"  # ✔ ✘
            pairs.append((delta, t.name, chain))

        time_length = max(len(p[0]) for p in pairs)
        task_length = max(len(p[1]) for p in pairs)
        task_print = [f"  {p[0]:>{time_length}} {p[1]:{task_length}} : {p[2]}" for p in pairs]

        messages = [
            "\n-< Welcome to Streamlet! >-",
            f"\nVersion:\t{metadata.version("streamlet")}",
            f"Configuration:\t{flow.path}",
            "Settings:",
            f"  Log Level       : {logging.getLevelName(Settings.log_level)}",
            f"  Celery Pool     : {Settings.celery_pool}",
            f"  Timezone        : {Settings.timezone}",
            f"  Allow Exec      : {check(Settings.allow_exec)}",
            f"  Outputs         : {check(not Settings.disable_outputs)}",
            f"  Print Traceback : {check(Settings.print_traceback)}",
            f"  Attr Seperator  : {Settings.nested_attr_seperator}",
            "\nConfigured Tasks:",
            *task_print,
            "",
        ]
        print("\n".join(messages), file=sys.__stdout__, flush=True)

    def on_consumer_ready(self, consumer):
        """This function is used to connect to the sources."""

        logger.debug("Preparing flow modules...")

        def start():
            for m in AbstractModule.all_modules.values():
                if m.enabled:
                    m.on_connect()

        start()
        logger.info("Flow modules are ready.")

        def shutdown():
            logger.info("Cleaning up and flushing modules...")
            for m in AbstractModule.all_modules.values():
                if m.enabled:
                    m.on_pre_shutdown()

            logger.info("Shutting down flow modules...")
            for m in AbstractModule.all_modules.values():
                if m.enabled:
                    m.on_shutdown()

            logger.info("Cleaned up streamlet. Exiting.")

        atexit.register(shutdown)
        logger.debug("Registered cleanup tasks.")

        super().on_consumer_ready(consumer)
        StreamletFlow.set_ready()
        logger.info("Setup successfully finished. Streamlet is ready.")
