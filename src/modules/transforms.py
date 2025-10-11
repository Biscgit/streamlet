"""Transform modules."""

import builtins
import importlib
import logging
import operator
import traceback
import typing
from datetime import datetime

from voluptuous import All, Any, Lower, Maybe, Optional

from core import Settings
from core.abstract import AbstractTransform
from core.modules import Importable
from core.validation import AlwaysList

if typing.TYPE_CHECKING:
    from core.metric import MetricFrame


@Importable
class CodeTransform(AbstractTransform):
    """Executes a Python code snippet on a Dataframe or Metric.
    Access it via `data` in the code."""

    def __init__(self, *args):
        super().__init__(*args)

        self.code = self.settings["src"]
        self.iter_over_dataframe = self.settings["mode"] == "metric"

        self.allowed_builtins = self.settings["builtins"]
        self.allowed_modules = {m: importlib.import_module(m) for m in self.settings["modules"]}

        self._binary = self.compile(self.code)

    @classmethod
    def params_schema(cls) -> dict:
        """
        :src: Python code to be executed
        :mod: Receive field data as either Dataframe or single Metric.
        :level: Compilation optimization level
        :modules: Additional modules to be loaded for use in code
        :builtins: Specify items from the builtins module (like str) for use in code
        """
        return {
            "src": str,
            "mode": All(Lower, Any("dataframe", "metric")),
            Optional("level", default=1): int,
            Optional("modules", default=[]): AlwaysList(str),
            Optional("builtins", default=[]): AlwaysList(str),
        }

    def __call__(self, data: "MetricFrame"):
        allowed_globals = self._allowed_globals()

        if self._binary:
            access = data if self.iter_over_dataframe else [data]
            for frame in access:
                allowed_locals = {"data": frame}

                try:
                    # pylint: disable=W0122
                    exec(self._binary, allowed_globals, allowed_locals)
                except Exception as e:
                    try:
                        tbc = traceback.extract_tb(e.__traceback__)[-1]
                        logging.error(
                            "Encountered %s: %s\n[%s] %s\n   %s%s",
                            e.__class__.__name__,
                            str(e),
                            tbc.lineno,
                            self.code.splitlines()[tbc.lineno - 1],
                            " " * (tbc.colno + len(str(tbc.lineno))),
                            "^" * (tbc.end_colno - tbc.colno),
                        )
                    # pylint: disable=W0718
                    except Exception as exc:
                        logging.error(
                            "[%s] Failed to generate code traceback: %s",
                            *(exc.__class__.__name__, str(exc)),
                        )
                        logging.error("Original [%s]: %s", e.__class__.__name__, str(e))

                    raise e

            for metric in data:
                metric.validate()

    def compile(self, code: str):
        """Compiles passed python code into a binary."""
        if code:
            if not Settings.allow_exec:
                msg = "Code execution setting is disabled by default because of security reasons."
                self.logger.error(msg)
                raise ValueError("Code execution not allowed.")

            try:
                return compile(code, "<string>", "exec", optimize=self.settings["level"])

            except SyntaxError as e:
                self.logger.error("Syntax error while compiling: %s", (str(e)))
                raise e

        return None

    def _allowed_globals(self) -> dict:
        """Holds the list of allowed globals accessible with `exec`."""
        modules = self.allowed_modules
        ok_builtins = {n: getattr(builtins, n) for n in self.allowed_builtins}
        miscellaneous_methods = {
            "print": self.logger.info,
            "log_info": self.logger.info,
            "log_warning": self.logger.warning,
            "log_error": self.logger.error,
        }
        return {"__builtins__": None, **miscellaneous_methods, **ok_builtins, **modules}

    # def log_err(self, msg: str, lineno: int, c_start: int, c_lgt: int) -> None:
    #     """Method should be used to display more information on errors inside `exec`."""
    #
    #     try:
    #         line = self.code.splitlines()[lineno - 1]
    #
    #         logging.error("Encountered %s at line [%s:%s]. Source:", msg, lineno, c_start)
    #         logging.error("]> %s", line)
    #         logging.error("%s%s", " " * (3 + c_start), "^" * max(c_lgt, 1))
    #
    #     except IndexError:
    #         pass


@Importable
class KeyMapping(AbstractTransform):
    """A simple key value mapper with yaml supported types.
    For more complex mapping operations, use the CodeTransform Module."""

    YAML_TYPES = (None, bool, int, float, str, list, dict, datetime)
    TYPE_MAP = {t.__name__: t for t in [str, int, bool, float]}

    def __init__(self, *args):
        super().__init__(*args)
        self._mapping = self.settings["mapping"]

        if self.settings["default"] is not None:
            self.settings["handle_missing"] = "default"

    def __call__(self, data: "MetricFrame"):
        key_field = self.settings["key"]
        result_field = self.settings["result_field"] or key_field
        cast = self.TYPE_MAP.get(self.settings["cast_key"], lambda t: t)

        for metric in data:
            try:
                key = cast(metric[key_field])
            except KeyError as e:
                self.logger.error("Key `%s` does not exist in Task `%s`.", key_field, data.name)
                raise e

            try:
                metric[result_field] = self._mapping[key]

            except KeyError as e:
                if (error_handler := self.settings["handle_missing"]) == "default":
                    metric[result_field] = self.settings["default"]
                elif error_handler == "original":
                    metric[result_field] = metric[key_field]
                elif error_handler == "original_casted":
                    metric[result_field] = key
                elif error_handler == "raise":
                    raise e

    @classmethod
    def params_schema(cls) -> dict:
        """
        :mapping: The key value mapping
        :key: The field from metrics to use as key. Use metric_field_name to access the metric
        :result_field: Field to which the result is being written to. `key`'s field by default
        :cast_key: Cast the key into a type before resolving on mapping
        :default: Default value if nothing found in mapping
        :handle_missing: Mode of handling missing keys, set to default if `default` set to not None
        """

        modes = ("default", "original", "original_casted", "raise", "nothing")
        return {
            "mapping": {Any(*cls.YAML_TYPES): Any(*cls.YAML_TYPES)},
            "key": str,
            Optional("result_field", default=None): Maybe(str),
            Optional("cast_key", default=None): Any(Lower, *cls.TYPE_MAP.keys()),
            Optional("default", default=None): Any(*cls.YAML_TYPES),
            Optional("handle_missing", default="raise"): Any(Lower, *modes),
        }


@Importable
class SimpleFilter(AbstractTransform):
    """Simple filter that filters data based on a condition from operators module.
    Define conditions in the format: `Condition` `Value` [`Data field`].
    Data gets excluded if all conditions are met.

    Leave the data field empty for using metric as default field."""

    conditions = {
        "lt": operator.lt,
        "le": operator.le,
        "eq": operator.eq,
        "ne": operator.ne,
        "ge": operator.ge,
        "gt": operator.gt,
        "in": operator.contains,
        "nn": lambda x, _: x is not None,
    }

    def __init__(self, *args):
        super().__init__(*args)
        self._cond, self._value, *self._data_field = self.settings["cond"].split(" ", maxsplit=2)
        self.mode = self.settings["mode"]

        if len(self._data_field) == 0:
            self._data_field = None
        else:
            self._data_field = self._data_field[0]

        try:
            self._cond = self.conditions[self._cond]
        except KeyError as e:
            conditions = list(self.conditions.keys())
            self.logger.error("Condition %s is unknown, possible are: %s", self._cond, conditions)
            raise ValueError("Unknown condition for comparison.") from e

    def __call__(self, data: "MetricFrame"):
        to_delete = []
        for metric in data:
            value = metric[self._data_field] if self._data_field else metric.metric

            try:
                comp_value = type(value)(self._value)
            except (ValueError, TypeError):
                comp_value = self._value

            result = self._cond(value, comp_value)
            if (self.mode == "keep" and result) or (self.mode == "drop" and not result):
                continue

            to_delete.append(id(metric))

        data[:] = [m for m in data.copy() if id(m) not in to_delete]

    @classmethod
    def params_schema(cls) -> dict:
        """
        :cond: Condition for checking Metrics
        :mode: Drop of Keep the items if the condition is true
        """
        return {"cond": str, "mode": All(Lower, Any("drop", "keep"))}
