"""This module is responsible for loading all the modules from the `modules` folder."""

import importlib
import inspect
import pkgutil
from typing import Generator, TypeVar

from voluptuous import LiteralInvalid, Union

from core.abstract import AbstractInput, AbstractModule, AbstractOutput, AbstractTransform
from core.logger import get_logger

T = TypeVar("T")
logger = get_logger("mods")


class Importable:
    """Make a module importable by adding this as a decorator."""

    IMPORTABLE = (AbstractInput, AbstractTransform, AbstractOutput)

    def __init__(self, *args, **kwargs):
        pass

    def __new__(cls, *args, **kwargs):
        # Create default instance if no cls is not called
        if args and issubclass(args[0], AbstractModule):
            instance = super().__new__(cls)
            return instance(args[0])

        return super().__new__(cls, *args, **kwargs)

    def __call__(self, cls):
        if not issubclass(cls, Importable.IMPORTABLE):
            logger.error("Module %s is not importable as it is of type %s", cls, type(cls))
            raise ImportError("Importing module of incorrect type.")

        if cls.importable:
            logger.error("Module %s is already importable. This can lead to unintended errors", cls)
            raise ValueError("Double applying of importable module changes.")

        cls.importable = True
        return cls


class Modules:
    """This class loads and is an interface to all loaded modules."""

    input_modules: dict[str, type["AbstractInput"]] = {}
    output_modules: dict[str, type["AbstractOutput"]] = {}
    transform_modules: dict[str, type["AbstractTransform"]] = {}

    @classmethod
    def __len__(cls):
        return len(cls.input_modules) + len(cls.transform_modules) + len(cls.output_modules)

    @classmethod
    def initialize(cls, package: str = None, file: str = None):
        """Loads the extensions and processes them."""

        logger.debug("Initializing modules...")
        for node_cls in cls._iter_module_cls(package, file):
            if issubclass(node_cls, AbstractInput):
                cls._add_module_cls(node_cls, cls.input_modules)
            elif issubclass(node_cls, AbstractTransform):
                cls._add_module_cls(node_cls, cls.transform_modules)
            elif issubclass(node_cls, AbstractOutput):
                cls._add_module_cls(node_cls, cls.output_modules)
            else:
                raise ValueError("Invalid class provided")

        logger.info("Successfully initialized %s modules.", cls.__len__())

    @classmethod
    def get_module_cls(cls, pipe_type: type[T], pipe_id: str) -> type[T]:
        """Returns the corresponding node class to the type and class id"""
        try:
            pipe_type = pipe_type.__name__.lower().removeprefix("abstract")
            return cls.__getattribute__(cls, f"{pipe_type}_modules")[pipe_id]

        except KeyError as e:
            raise LiteralInvalid(f"Module of name `{pipe_id}` is unknown", ["type"]) from e

        except AttributeError as e:
            raise ValueError(f"Module type of `{pipe_type}` is invalid.") from e

    @classmethod
    def _iter_module_cls(cls, pkg: str, file: str) -> Generator[type[AbstractModule], None, None]:
        """Loads all extensions from the specified folder."""
        assert pkg and file is None or pkg is None and file

        if pkg:
            package_module = importlib.import_module(pkg)
            path, name = package_module.__path__, f"{package_module.__name__}."

            for _, module_name, is_pkg in pkgutil.walk_packages(path, name):
                if is_pkg:
                    continue
                yield from cls._iter_module(module_name)

        if file:
            yield from cls._iter_module(file)

    @classmethod
    def _iter_module(cls, module_name: str):
        """Load a single module"""
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            logger.info(
                "Skipping module %s because of missing dependency `%s`. "
                "Check build and pyproject.toml if required.",
                *(module_name, e.name),
            )
            return

        for _, mod in inspect.getmembers(module, inspect.isclass):
            if cls._validate_module(mod):
                abs_type = [i for i in Importable.IMPORTABLE if issubclass(mod, i)][0].__name__
                logger.debug(
                    "Loaded module %s (%s) from %s.",
                    *(mod.module_name(), abs_type, module.__name__),
                )
                yield mod

    @staticmethod
    def _validate_module(mod: type[AbstractModule]) -> bool:
        """Some tests weather the module class has expected implementations."""
        if not (issubclass(mod, AbstractModule) and mod.importable):
            return False

        try:
            if not isinstance(mod.module_name(), str):
                logger.error("Module name is not a string.")
                return False

            if len(mod.__abstractmethods__) > 0:
                logger.error("Skipping %s as it is not fully implemented.", mod.__name__)
                return False

            if not isinstance(c := mod.connection_schema(), (dict, Union)):
                raise ValueError(f"Connection-Schema must be a dict or Union, not {type(c)}.")

            params = list(inspect.signature(mod.__call__).parameters.values())

            in_param = list(inspect.signature(AbstractInput.__call__).parameters.values())
            if issubclass(mod, AbstractInput):
                if params != in_param:
                    raise ValueError(
                        f"Module {mod} has an invalid __call__ signature. "
                        f"It should be {in_param}, not {params}."
                    )

                if not isinstance(c := mod.task_params_schema(), (dict, Union)):
                    raise ValueError(f"Task-Schema must be a dict or Union, not {type(c)}.")

            out_param = list(inspect.signature(AbstractTransform.__call__).parameters.values())
            if issubclass(mod, (AbstractOutput, AbstractTransform)):
                if params != out_param:
                    raise ValueError(
                        f"Module {mod} has an invalid __call__ signature. "
                        f"It should be {out_param}, not {params}."
                    )

                if not isinstance(c := mod.params_schema(), (dict, Union)):
                    raise ValueError(f"Params-Schema must be a dict or Union, not {type(c)}.")

            # check module documentation
            if not mod.__doc__:
                logger.warning("Module %s has no documentation.", str(mod))

            # check schema documentation
            for func in ["connection_schema", "task_params_schema", "params_schema"]:
                if hasattr(mod, func) and not getattr(mod, func).__doc__:
                    logger.warning("Module %s.%s has no documentation.", str(mod), func)

        except ValueError as e:
            logger.error("Not loading %s: %s", mod.__class__.__name__, str(e))
            return False

        return True

    @staticmethod
    def _add_module_cls(_cls: type[AbstractModule], mapping: dict):
        """Adds a new module class to the designated dict."""
        name = _cls.module_name()

        if name in mapping.keys():
            raise ValueError(f"Extension with id {name} already exists! Please define a custom id.")

        mapping[name] = _cls

    @staticmethod
    def create_module(_cls: type[T], flow, config: dict, index: int) -> T:
        """This function is used to create the input module."""

        mod = Modules.get_module_cls(_cls, config["type"])
        mod_instance = mod(flow, config, index)

        if mod_instance.enabled is False:
            logger.debug("Module %s is disabled, skipping.", mod_instance.name)

        return mod_instance
