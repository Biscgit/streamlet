"""Validation schemas for the configuration files."""

# pylint: disable=R0903
import copy
import math
from string import Template

from voluptuous import ALLOW_EXTRA, PREVENT_EXTRA, Lower, Maybe, TypeInvalid
from voluptuous.error import Invalid, MultipleInvalid, ValueInvalid
from voluptuous.schema_builder import Optional, Schema, Schemable
from voluptuous.validators import All, Boolean, Coerce, PathExists, Range

from core.abstract import *  # pylint: disable=W0401,W0614
from core.modules import Modules
from core.settings import Settings

from .validators import AlwaysList, Crontab, EnvironmentVar, ModuleName, TimeToSeconds


class HeaderSchema(Schema):
    """Schema for the header of a configuration file."""

    def __init__(self, required=True, extra=ALLOW_EXTRA):
        # pylint: disable=E1120
        s = {
            "flow": {
                "version": "v1",
                Optional("extends", default=[]): AlwaysList(All(EnvironmentVar(), PathExists())),
                Optional("settings", default={}): Optional(Settings.__annotations__),
            }
        }

        super().__init__(s, required=required, extra=extra)


class EnvSchema(Schema):
    """Schema for environment variables in configurations."""

    def __init__(self, required=True, extra=ALLOW_EXTRA):
        s = {Optional("env", default={}): Optional({str: EnvironmentVar()})}
        super().__init__(s, required=required, extra=extra)


class ModulesSchema(Schema):
    """Schema for a module configuration."""

    def __init__(self, required=True, extra=ALLOW_EXTRA):
        s = {
            "input": self.ModuleValidator(self, AbstractInput),
            Optional("transform", default=[]): self.ModuleValidator(self, AbstractTransform),
            "output": self.ModuleValidator(self, AbstractOutput),
        }
        super().__init__(s, required=required, extra=extra)

    class ModuleValidator:
        """Module Validator as a class to access the inner schema on errors."""

        def __init__(self, parent: Schema, mod_type: type[AbstractModule]):
            self.schema = []
            self.mod_type = mod_type
            self.last_index = None
            self._parent = parent

        def __repr__(self):
            return f"?{self.schema}"

        def __call__(self, configs):
            try:
                return self.validate(configs)
            except Invalid as e:
                # prepend last path for error finding in modules (as different)
                if self.last_index is not None:
                    e.prepend([self.last_index])
                raise e

        @staticmethod
        def base_schema() -> dict:
            """Base module schema."""
            # pylint: disable=E1120
            return {
                "type": ModuleName(),
                Optional("name", default=None): Maybe(ModuleName()),
                Optional("enabled", default=not Settings.disable_default): Boolean(),
            }

        def validate(self, configs):
            """Validate each module configuration."""
            required = self._parent.required

            if not isinstance(configs, list):
                raise TypeInvalid("not of type list.")

            configs: list = configs.copy()

            self.schema = [Schema(self.base_schema(), extra=ALLOW_EXTRA) for _ in configs]
            self.last_index = None

            for i, value in enumerate(configs):
                self.last_index = i
                base: dict = self.schema[i].schema

                # inclusion and exclusion of tasks
                conf_tasks = {
                    Optional("include_tasks", default=None): Maybe(AlwaysList(Lower)),
                    Optional("exclude_tasks", default=None): Maybe(AlwaysList(Lower)),
                }
                conf_inputs = {
                    Optional("include_inputs", default=None): Maybe(AlwaysList(Lower)),
                    Optional("exclude_inputs", default=None): Maybe(AlwaysList(Lower)),
                }

                value = Schema(base, required=True, extra=ALLOW_EXTRA)(value)

                # skip validation if disabled
                if Settings.skip_disabled_validation and not value["enabled"]:
                    configs[i] = value
                    continue

                # expand required module keys from defined type
                cls = Modules.get_module_cls(self.mod_type, value["type"])

                conn_schema = Schema(cls.connection_schema(), required=required)
                base |= self.optional_subschema("connection", conn_schema)

                # add task schema to Inputs
                if issubclass(cls, AbstractInput):
                    base[Optional("tasks", default=[])] = TaskSchema(cls, required=required)

                # add params to Transforms and Outputs
                if issubclass(cls, AbstractProcessor):
                    base |= self.optional_subschema("params", cls.params_schema())

                    # add exclusion filters and check
                    base |= conf_tasks | conf_inputs
                    All(KeyCount(conf_tasks, _max=1), KeyCount(conf_inputs, _max=1))(value)

                # Add dataframe manipulation to Outputs
                if issubclass(cls, AbstractIO):
                    modi_schema = {
                        Optional("time_modulus", default=1): EnvironmentVar(TimeToSeconds()),
                        Optional("time_offset", default=0): EnvironmentVar(TimeToSeconds()),
                    }
                    base |= self.optional_subschema("modifiers", modi_schema)

                # Add priority setting for Transforms
                if issubclass(cls, AbstractTransform):
                    base[Optional("priority", default=0)] = Range(min=-256, max=256)

                # overwrite config after finished validation
                final_schema = Schema(base, required=required, extra=PREVENT_EXTRA)
                configs[i] = final_schema(value)

                # add schema to list for error hinting
                self.schema[i] = final_schema

            return configs

        @staticmethod
        def optional_subschema(key: str, schema: Schemable, default=None) -> dict:
            """Marks keys as Optional if all keys of the schema are optional."""

            schema = schema.schema if hasattr(schema, "schema") else schema
            schemas = schema.validators if hasattr(schema, "validators") else [schema]

            for s in schemas:
                assert isinstance(s, dict)

                if all(isinstance(k, Optional) for k in s.keys()):
                    default = default or {k.schema: k.default() for k in s.keys()}
                    return {Optional(key, default=default): schema}

            return {key: schema}


class FlowSchema(Schema):
    """Full schema for a Flow configuration file."""

    def __init__(self, required=True, extra=PREVENT_EXTRA):
        s = {
            **HeaderSchema(required=required, extra=extra).schema,
            **EnvSchema(required=required, extra=extra).schema,
            **ModulesSchema(required=required, extra=extra).schema,
        }
        super().__init__(s, required=required, extra=extra)


class TaskSchema(Schema):
    """Schema for a Task defined in an Input."""

    def __init__(self, module: type[AbstractInput], required=True, extra=PREVENT_EXTRA):
        task_params = module.task_params_schema()
        params = ModulesSchema.ModuleValidator.optional_subschema("params", task_params)

        s = {
            "name": ModuleName(),
            "cron": Crontab(),
            Optional("result", default={}): {
                Optional("metrics", default=["metric"]): Maybe(AlwaysList(str)),
                Optional("attributes", default=None): Maybe(AlwaysList(str)),
            },
            Optional("static_attributes", default={}): Maybe({str: object}),
            Optional("enabled", default=True): Boolean(),  # pylint: disable=E1120
            Optional("max_retries", default=0 if Settings.run_once else 2): int,
            Optional("retry_delay", default=0 if Settings.run_once else 10): TimeToSeconds(),
        }
        s |= params | self.repeat_schema()

        super().__init__(s, required=required, extra=extra)
        self.schema = [s]

    def __call__(self, config):
        results = []

        def render(value, parameters):
            """Expands strings from passed parameters."""
            for key, item in iter(value.items() if isinstance(value, dict) else enumerate(value)):
                if isinstance(item, (list, dict)):
                    render(item, parameters)

                if isinstance(item, str):
                    value[key] = Template(item).safe_substitute(**parameters)

        for i, task in enumerate(config):
            try:
                validated = Schema(self.repeat_schema(), extra=ALLOW_EXTRA)(task)

                # validate directly if no repeat added
                if (for_each := validated["repeat_for"]) is None:
                    results.append(super().__call__(task))
                    continue

                keys = list(for_each.keys())
                length = for_each[keys[0]]

                # generate a list of each rendered configuration
                for j in range(len(length)):
                    each_conf = copy.deepcopy(task)
                    params = {key: for_each[key][j] for key in keys} | {"i": j}

                    render(each_conf, params)
                    results.append(super().__call__(each_conf))

            except Invalid as e:
                e.prepend([i])
                raise e

        return results

    @staticmethod
    def repeat_schema() -> dict:
        """Schema for configuring task repeating."""

        def check_length(config) -> dict[str, list[str]]:
            """Ensure the length of each item is the same."""

            config: dict = Schema({str: AlwaysList(Coerce(str))})(config)

            length = None
            first = None

            for k, v in config.items():
                if length is None:
                    first = k
                    length = len(v)

                if length != len(v):
                    msg = f"Length of {k} ({len(v)}) is unequal to {first} ({length})"
                    raise ValueInvalid(msg, path=[k])

            return config

        return {Optional("repeat_for", default=None): Maybe(check_length)}


class KeyCount(Schema):
    """Validate weather the number of all specified keys is within a given range."""

    def __init__(self, schema: Schemable, _min=0, _max=math.inf):
        self.base_schema = schema
        self.keys = [key.schema if hasattr(key, "schema") else key for key in schema]
        self.range = [_min, min(_max, len(self.keys))]

        if any(not isinstance(key, str) for key in self.keys):
            raise ValueError("Only string keys are supported!")

        s = All(self.validate_count, self.base_schema)
        super().__init__(s, required=True, extra=ALLOW_EXTRA)

    def validate_count(self, v):
        """Executes validation with specified constraints."""
        count = sum(key in v for key in self.keys)
        _min, _max = self.range

        if _min <= count <= _max:
            return v

        err_msg = f"Only {_min} to {_max} items of {list(self.keys)} can be present"
        raise MultipleInvalid([ValueInvalid(err_msg, path=[key]) for key in self.keys])
