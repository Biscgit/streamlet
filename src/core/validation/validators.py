"""Validators for the variables and structures."""

# pylint: disable=R0903
import os
import re
import typing
from string import ascii_letters, digits

import simplejson as json
from voluptuous import PREVENT_EXTRA
from voluptuous.error import ValueInvalid
from voluptuous.schema_builder import Schema
from voluptuous.validators import All, Coerce

from core.helpers import parse_cron

__all__ = ["JsonString", "Crontab", "ModuleName", "AlwaysList", "EnvironmentVar", "TimeToSeconds"]


class JsonString:
    """Schema for a JSON string.

    This schema validates that the value is a valid JSON string.
    It returns the original string if `load` is set to False, otherwise the loaded python object.
    """

    def __init__(self, load=True):
        self._load = load

    def __call__(self, value):
        """Validate the JSON string."""
        try:
            if isinstance(value, str):
                value = json.loads(value)
            dumped = json.dumps(value)

        except json.JSONDecodeError as e:
            raise ValueInvalid(f"Value is not JSON valid: {e.msg} at [{e.lineno}:{e.colno}]") from e

        return value if self._load else dumped

    def __repr__(self):
        return f"JsonString(load={self._load})"


class Crontab:
    """Schema for a cron expression."""

    @staticmethod
    def __call__(value):
        """Validate the cron expression."""
        try:
            value = str(value)
            parse_cron(value)
            return value
        except Exception as e:
            raise ValueInvalid(f"`{value}` is not a valid string with cron syntax") from e

    def __repr__(self):
        return "Crontab()"


class ModuleName:
    """Allowed characters for module and task names."""

    def __init__(self, allow_uppercase: bool = False):
        self.allow_uppercase = allow_uppercase

    def __call__(self, value):
        if not isinstance(value, str):
            raise ValueInvalid(f"Module name `{value}` must be a string.")

        if not self.allow_uppercase:
            value = value.lower()

        allowed = ascii_letters + digits + "_-.+"

        if all(c in allowed for c in value):
            return value

        raise ValueInvalid(f"Module `{value}` contains invalid characters. Allowed: `{allowed}`.")

    def __repr__(self):
        return "str [ModuleName]"


class AlwaysList(Schema):  # pylint: disable=R0903
    """The final result will be a list.

    You can set this field to either a single value if there is only one item
    or a list of the same value types."""

    def __init__(self, schema=object, required=True, extra=PREVENT_EXTRA):
        _s = Coerce(lambda s: [s] if not isinstance(s, list) else s)
        if schema:
            _s = All(_s, [schema])

        super().__init__(_s, required=required, extra=extra)
        self.schema = [schema]

    def __repr__(self):
        return f"!{self.schema}"


class EnvironmentVar:
    """Schema for expanding environment variables.

    Use the `${...}` syntax to define environment variables, which are loaded on runtime.

    It is also possible to nest env variable that holds others,
    as these are evaluated recursively (with a max depth of 16)."""

    def __init__(self, return_type: typing.Callable = str, max_depth=16):
        assert isinstance(max_depth, int)
        self._max_depth = max_depth
        self.schema = return_type

    def __call__(self, value: str, max_depth=None) -> str:
        """Recursively expand environment variables."""
        if max_depth is None:
            max_depth = self._max_depth

        if max_depth < 0:
            raise ValueInvalid("Max depth reached while expanding environment variables.")

        value = str(value)
        if (new := os.path.expanduser(os.path.expandvars(value))) == value:
            return self.schema(value)
        return self(new, max_depth - 1)

    def __repr__(self):
        return "str [Environment]"


class TimeToSeconds:
    """Schema for converting time strings to seconds.

    By default, the field accepts a decimal or a float specifying the amount of time in seconds.

    Here you can also set higher time spans, such as `1h` (for one hour) with
    [int|decimal] + [unit].
    Possible options are:
    `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d` (days).

    Combine multiple units with `:`, e.g. `1h:30m:15s` for 1 hour, 30 minutes and 15 seconds.
    """

    @staticmethod
    def __call__(value: str | int | float) -> int | float:
        """Transforms a time string to seconds."""
        if isinstance(value, str):
            if value.removeprefix("-").isdigit():
                return int(value)
            if value.removeprefix("-").replace(".", "", 1).isdigit():
                return float(value)

            t_parts = value.split(":")
            result = 0

            for part in t_parts:
                if not part.strip():
                    continue

                try:
                    match = re.search(r"(\d+)([a-zA-Z]+)", part.lower())
                    val, unit = match.group(1), match.group(2)

                    factors = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
                    result += (float(val) if val.isdecimal() else int(int)) * factors[unit]

                except Exception as e:
                    err_msg = f"Invalid time format `{part}` not parsable!"
                    raise ValueInvalid(err_msg, path=[]) from e

            value = -result if value.startswith("-") else result

        return value

    def __repr__(self):
        return "Duration()"
