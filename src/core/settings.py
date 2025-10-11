"""Contains Streamlet configuration specific classes."""

import logging
import os
from datetime import datetime

from voluptuous import Any, Boolean, Maybe, Number, Schema

from core.validation import EnvironmentVar, TimeToSeconds


# pylint: disable=E1120
class Settings:
    """Base class for easier configuration overview and processing.
    For each setting, a default value and type is required!"""

    class Persistent:
        """Holds variables that cannot be changed anymore."""

        keys = []

        @classmethod
        def add(cls, key):
            """Adds a key to persistent list."""
            cls.keys.append(key)

        def __contains__(self, item):
            return item in self.keys

    # Streamlet configuration
    config: EnvironmentVar() = "/etc/streamlet/flow.yaml"
    allow_exec: Boolean() = False
    skip_disabled_validation: Boolean() = False
    nested_attr_seperator: str = "."
    task_name_prefix: EnvironmentVar() = ""
    task_id_queue_size: int = 1024
    allow_none_metric: Boolean() = False
    default_cert_path: Maybe(EnvironmentVar()) = "/etc/ssl/certs/bundle.crt"
    hide_welcome: Boolean() = False
    timezone: EnvironmentVar() = "UTC"

    # Readiness configuration
    disable_readiness_probe: Boolean() = False
    readiness_port: int = 5012

    # Debug configuration
    only_validate: Boolean() = False
    run_once: Boolean() = False
    print_config: Boolean() = False
    print_traceback: Boolean() = False
    disable_outputs: Boolean() = False
    disable_default: Boolean() = False

    # Logging configuration
    log_level: int = logging.INFO
    log_file: Maybe(EnvironmentVar()) = "/tmp/streamlet.log"
    log_format: str = "[%(asctime)s: %(emoji)s %(short_level)s/%(name)s] %(mod_name)s%(message)s"

    # Celery configuration
    celery_pool: Any("solo", "threads") = "threads"
    celery_broker: EnvironmentVar() = "redis://localhost:6379/0"
    celery_backend: EnvironmentVar() = "redis://localhost:6379/1"
    celery_beat_file: EnvironmentVar() = f"/tmp/streamlet_beat-{datetime.now().timestamp()}"
    celery_log_level: int = logging.WARNING
    celery_concurrency: int = 16
    celery_result_expire: TimeToSeconds() = "1d"

    @classmethod
    def initiate(cls):
        """Load streamlet config from environment variables."""
        mapping = {int: lambda v: int(Schema(Number(scale=0))(v))}

        # load all fields as env vars automatically
        for field, t in cls.__annotations__.items():
            # load env var value if exists, otherwise verify default
            if not (value := os.getenv(f"STREAMLET_{field.upper()}")):
                value = getattr(cls, field)

            validator = mapping.get(t) or t
            setattr(cls, field, validator(value))

    @classmethod
    def set(cls, key, value, persistent=False):
        """Set an option with type checks."""
        if key in Settings.Persistent():
            logging.getLogger("flow").debug("Skipping overwriting settings of argv parameters.")
            return

        value = cls.__annotations__[key](value)
        setattr(cls, key, value)

        # handle overwriting of preconfigured loggers
        if key == "log_level":
            logging.getLogger().setLevel(value)
            logging.getLogger("flow").setLevel(value)
            logging.getLogger("mods").setLevel(value)

        if persistent:
            Settings.Persistent.add(key)

    @classmethod
    def extend(cls, mapping: dict[str, object]):
        """Set multiple settings from a dict"""
        for pair in mapping.items():
            cls.set(*pair)


def setup():
    """Sets up default settings."""
    Settings.initiate()


setup()
