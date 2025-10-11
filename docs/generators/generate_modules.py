# pylint: skip-file
"""Generate documentation for all modules and validators in Streamlet."""
import inspect
import logging
import math
import os

import simplejson as json
import yaml
from voluptuous import Any, Clamp, Coerce, Optional

from core.abstract import AbstractModule
from core.modules import Modules
from core.validation import EnvironmentVar, validators

logging.basicConfig(level=logging.INFO)


def serialize_schema(schema: dict, no_indent=False) -> str:
    def jsonify(data):
        if not isinstance(data, dict):
            # handle iterables
            if isinstance(data, (list, tuple)):
                return map(jsonify, data)
            # check EnvVar
            if isinstance(data, EnvironmentVar):
                return f"EnvironmentVar({jsonify(data.schema)})"
            # unpack nested
            if hasattr(data, "validators"):
                return {f"<{data.__class__.__name__.upper()}>": jsonify(data.validators)}
            # check Clamp validator
            if isinstance(data, Clamp):
                return f"({data.min or -math.inf} TO {data.max or math.inf})"
            # check nested
            if hasattr(data, "schema"):
                subs = serialize_schema(data.schema, no_indent=True)
                return {f"<{data.__class__.__name__.upper()}>": jsonify(data.schema)}
            # check special functions
            methods = ["Boolean", "Lower", "PathExists", "Upper"]
            if hasattr(data, "__name__") and data.__name__ in methods:
                return f"{data.__name__}()"
            # format types
            if isinstance(data, type):
                return data.__name__
            # format static strings
            if isinstance(data, str):
                return f"{data} [str]"
            # handle Coerce validator
            if isinstance(data, Coerce):
                return f"{jsonify(data.type)}!"

            # return default
            return data

        new_data = {}
        for k, v in data.items():
            # check Optional
            if isinstance(k, Optional):
                default = k.default() if callable(k.default) else "<NO DEFAULT>"
                k = f"{k}?({default})"
            if isinstance(k, type):
                k = jsonify(k)
            if isinstance(k, Any):
                k = f"<ANY>({"|".join(str(jsonify(j)) for j in k.validators)})"
            new_data[k] = jsonify(v)
        return new_data

    out = json.dumps(jsonify(schema), default=str, iterable_as_array=True, allow_nan=True)
    return out if no_indent else f"```yaml\n{yaml.dump(json.loads(out))}```\n"


def expandable(title: str, description: str):
    return f"<details>\n<summary>{title}</summary>\n\n{description}\n</details>\n\n"


def get_mod_location(mod: type) -> tuple[str, str]:
    full_path = inspect.getfile(mod)
    file_name = full_path.rsplit("/", maxsplit=1)[-1]
    truncated = full_path.removeprefix(os.getcwd())
    return f"{file_name}:{inspect.getsourcelines(mod)[1]}", f"..{truncated}"


def schema_doc(text: str | None, obj: dict) -> str:
    if text is None:
        return "No description provided"

    results = []
    for item in text.strip().splitlines():
        try:
            _, key, description = item.split(":", maxsplit=2)
            if [k for k in obj.keys() if k == key and isinstance(k, Optional)]:
                key = f"{key.strip()}?"
            elif key in obj:
                key = f"<u>{key.strip()}</u>"
            else:
                continue
            results.append(f"- **{key}:** {description.strip()}")

        except ValueError:
            results.append(f"- {item}")

    return "\n".join(results)


def print_module(mod: type[AbstractModule]) -> str:
    text = f"### [{mod.module_name(lower=False)}](#list-of-all-modules)\n\n"

    name, path = get_mod_location(mod)
    text += f"*See implementation at [{name}]({path})*\n\n"

    text += f"#### Description:\n\n{mod.__doc__}\n\n"
    footer = ""

    # get connection
    schemas = getattr((schema := mod.connection_schema()), "validators", [schema])
    for i, s in enumerate(schemas, start=1):
        text += f"#### Connection{f" (Option {i})" if len(schemas) > 1 else ""}:\n\n"
        text += f"{schema_doc(mod.connection_schema.__doc__, s)}\n\n"
    footer += expandable("Connection Validator", serialize_schema(schema))

    # get task params
    if hasattr(mod, "task_params_schema"):
        schemas = getattr((schema := mod.task_params_schema()), "validators", [schema])
        for i, s in enumerate(schemas, start=1):
            text += f"#### Task Parameters{f" (Option {i})" if len(schemas) > 1 else ""}:\n\n"
            text += f"{schema_doc(mod.task_params_schema.__doc__, s)}\n\n"
        footer += expandable("Task Parameter Validator", serialize_schema(schema))

    # get params
    if hasattr(mod, "params_schema"):
        schemas = getattr((schema := mod.params_schema()), "validators", [schema])
        for i, s in enumerate(schemas, start=1):
            text += f"#### Parameters{f" (Option {i})" if len(schemas) > 1 else ""}:\n\n"
            text += f"{schema_doc(mod.params_schema.__doc__, s)}\n\n"
        footer += expandable("Parameter Validator", serialize_schema(schema))

    return f"{text}{footer}\n\n"


if __name__ == "__main__":
    logging.info("Starting...")

    Modules.initialize(package="modules")
    logging.info("Successfully loaded %s modules.", Modules.__len__())

    with open("./docs/MODULES.md", "w") as f:
        logging.info("Generating and writing documentation...")

        f.write(
            "*This file is automatically generated.*\n\n"
            "# Modules\n\n"
            "This document lists all available modules with their configuration mapping.\n\n"
            "All variables that have their name as `<...>` are respective sub rules.\n\n"
            "Keys ending with ? are optional, while underlined keys are required to set.\n\n"
        )

        f.write(
            "### Reading Raw Validators\n\n"
            "The raw YAML validators show the configuration structure, with specialities:\n\n"
            "- `<type>`: All sub items are to be interpreted as the defined type.\n\n"
            "- `type!`: Sub items get converted by this operator in list order.\n\n"
            "- `value [type]`: Validator with specific value requested.\n\n"
            "- `type()`: Item gets checked against type operator.\n\n"
            "- `type(subtype)`: Item gets converted into subtype.\n\n"
            "- `type?(default)`: Optional keys with their default argument.\n\n"
        )

        f.write(
            "### Custom Validators\n\n"
            "There are custom validators that make defining fields easier.\n\n"
        )
        for _, vld in inspect.getmembers(validators, inspect.isclass):
            if vld.__module__ == validators.__name__:
                docs = "\n  ".join(vld.__doc__.splitlines())
                f.write(f"- **{vld.__name__}** (*shown as `{vld().__repr__()}`*):\n\n  {docs}\n\n")

        f.write("## List of all Modules\n\nJump to any module by clicking on the following:\n\n")
        for pipe in ["Input", "Transform", "Output"]:
            f.write(f"- [{pipe}s](#{pipe.lower()}s)\n\n")
            for mod in getattr(Modules, f"{pipe.lower()}_modules").values():
                f.write(f"  - [{mod.module_name(lower=False)}](#{mod.module_name()})\n\n")

        f.write("## [Inputs](#list-of-all-modules)\n\n")
        for mod in Modules.input_modules.values():
            f.write(print_module(mod))

        f.write("## [Transforms](#list-of-all-modules)\n\n")
        for mod in Modules.transform_modules.values():
            f.write(print_module(mod))

        f.write("## [Outputs](#list-of-all-modules)\n\n")
        for mod in Modules.output_modules.values():
            f.write(print_module(mod))

    logging.info("Documentation generated.")
