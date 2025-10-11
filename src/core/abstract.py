"""Module holds the abstract classes for other modules."""

import typing
from abc import ABC, abstractmethod
from base64 import b64encode
from datetime import datetime
from fnmatch import fnmatch
from hashlib import blake2s

import simplejson as json
from voluptuous import Union

from core.logger import get_logger
from core.settings import Settings
from core.task import StreamletTask

if typing.TYPE_CHECKING:
    from core.flow import StreamletFlow
    from core.metric import MetricFrame

__all__ = [
    "AbstractModule",
    "AbstractIO",
    "AbstractProcessor",
    "AbstractInput",
    "AbstractTransform",
    "AbstractOutput",
]


class AbstractModule(ABC):
    """This is the base class for all modules."""

    all_modules: dict[str, "AbstractModule"] = {}
    importable = False

    def __init__(self, flow: "StreamletFlow", config: dict, index: int):
        self.flow = flow

        self._full_config = config
        self.index: int = index

        self.connection_config = config["connection"]

        # set name to config has if not provided
        self.name = config["name"]
        if self.name is None:
            _hash = blake2s(json.dumps(config, sort_keys=True).encode()).digest()
            self.name = f"{self.module_name()}_{b64encode(_hash, b"-+").decode().rstrip("=")[:8]}"

        if self.name in AbstractModule.all_modules:
            raise ValueError(f"Module with name {self.name} already exists.")

        # setup logger
        self.logger = get_logger(f"<{self.name}>", "mods")

        AbstractModule.all_modules[self.name] = self

    @property
    def enabled(self) -> bool:
        """Returns whether the module is enabled."""
        return self._full_config["enabled"]

    @classmethod
    def module_name(cls, lower: bool = True):
        """Returns the class id of the module."""
        name = cls.__name__.removesuffix("Input").removesuffix("Output")
        return name.lower() if lower else name

    def on_connect(self) -> None:
        """This function is used to connect to the source."""
        return None

    def on_pre_shutdown(self) -> None:
        """This function is used to prepare the module for shutdown, e.g., flushing buffers."""
        return None

    def on_shutdown(self) -> None:
        """This function is used to disconnect from the source."""
        return None

    @classmethod
    def connection_schema(cls) -> dict | Union:
        """Returns the schema for the configuration of the connection."""
        return {}

    @abstractmethod
    def __call__(self, *args, **kwargs):
        """Ensure all modules have a callable function."""


class AbstractIO(AbstractModule, ABC):
    """This is the base class for all io modules."""

    def __init__(self, *args):
        super().__init__(*args)
        self.modifiers = self._full_config["modifiers"]

    def modify(self, data: "MetricFrame") -> None:
        """This manipulates the data from modifier schema.
        This method is not intended for overwriting!"""

        timestamp = data.creation_timestamp.timestamp()

        if modulus := self.modifiers["time_modulus"]:
            timestamp -= timestamp % modulus
        if offset := self.modifiers["time_offset"]:
            timestamp -= offset

        data.creation_timestamp = datetime.fromtimestamp(timestamp)


class AbstractInput(AbstractIO, ABC):
    """This is the base class for all input modules."""

    def __init__(self, *args):
        super().__init__(*args)

        self.task_confs = self._full_config["tasks"]
        self.tasks = [StreamletTask(self, conf) for conf in self.task_confs]

    @classmethod
    def task_params_schema(cls) -> dict | Union:
        """Returns the schema for the configuration of the transform."""
        return {}

    @abstractmethod
    def __call__(self, params: dict) -> list[dict]:
        """This is the function executed for the task.
        It is left for each module to be implemented.

        Args:
            params (dict): Takes the configured Task parameters.

        Returns:
            list[dict]: Results of the call to be processed as metrics.
                Return multiple dicts in a list for multiple metrics.
        """


class AbstractProcessor(AbstractModule, ABC):
    """This is the base class for all transformer modules."""

    def __init__(self, *args):
        super().__init__(*args)
        self.settings = self._full_config["params"]

        filter_keys = ["include_inputs", "exclude_inputs", "include_tasks", "exclude_tasks"]
        self.filters = {k: v for k, v in self._full_config.items() if k in filter_keys}

    @classmethod
    def params_schema(cls) -> dict | Union:
        """Returns the schema for the configuration of the transform."""
        return {}

    def accepts_from(self, input_name: str, task_name: str, ignore_enabled: bool = False) -> bool:
        """Checks if the output accepts from the input."""

        if self.enabled is False and not ignore_enabled:
            return False

        if (fi := self.filters["include_inputs"]) is not None:
            if not any(fnmatch(input_name, p) for p in fi):
                return False

        if (fi := self.filters["exclude_inputs"]) is not None:
            if any(fnmatch(input_name, p) for p in fi):
                return False

        if (fi := self.filters["include_tasks"]) is not None:
            if not any(fnmatch(task_name, p) for p in fi):
                return False

        if (fi := self.filters["exclude_tasks"]) is not None:
            if any(fnmatch(task_name, p) for p in fi):
                return False

        return True

    @abstractmethod
    def __call__(self, data: "MetricFrame"):
        """This is the function executed for the task."""


class AbstractTransform(AbstractProcessor, ABC):
    """This is the base class for all transform modules."""

    @property
    def priority(self):
        """Returns a set priority for a Transform."""
        return self._full_config["priority"]


class AbstractOutput(AbstractIO, AbstractProcessor, ABC):
    """This is the base class for all output modules."""

    @property
    def enabled(self) -> bool:
        """Returns whether the module is enabled."""
        return self._full_config["enabled"] and not Settings.disable_outputs
