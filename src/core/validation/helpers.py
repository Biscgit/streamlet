"""Helper methods for validation."""

# pylint: disable=R0903
import difflib
import logging
import sys

from voluptuous.error import Invalid, MultipleInvalid
from voluptuous.schema_builder import Schemable


def walk_similar_key(schema, config, path, key):
    """Finds a matching key to the provided path's key."""
    group = schema.schema

    for p in path:
        if isinstance(config, list):
            # list validators have only one element if all are same, handling with min:
            group = group[min(int(p), len(group) - 1)]
        else:
            group = group[p]

        # get internal Schemable
        while hasattr(group, "schema"):
            group = getattr(group, "schema")

        try:
            config = config[p]
        except KeyError:
            break

    keys = [str(k) for k in group.keys()] if isinstance(group, dict) else []

    if matches := difflib.get_close_matches(str(key), keys, n=1, cutoff=0.5):
        config = str(config[key]).encode("unicode_escape").decode()
        config = f"{config[:55]}..." if len(config) > 55 else config
        return matches, config

    return None


def print_validation_error(schema, configuration, errors):
    """Display a formatted explanation of an exception with its schema options."""

    errors = errors.errors if isinstance(errors, MultipleInvalid) else [errors]
    logging.debug("Processing errors...")

    for error in errors:
        # extract invalid item's path
        try:
            *path, key = error.path
            field = "".join(map(lambda s: f"[{s}]", ["<>", *path])) + f" > {key}"
        except ValueError:
            path, key = [], None
            field = "No error path found!"

        # print incorrect field
        cls = f"[{error.__class__.__name__}] "
        print(f"{cls:╴<25}┬🠆 Field: {field}", file=sys.__stdout__, flush=True)

        # field resolve for incorrect keys
        if error.msg == "extra keys not allowed" and key:
            if found := walk_similar_key(schema, configuration.copy(), path, key):
                matches, config = found
                msg = f"{error.msg}. Did you mean: `{matches[0]}: {config}`?"

            else:
                msg = f"{error.msg}. No close matches found for `{key}`"

            error.args = (msg, *error.args[1:])  # Update the error message

        # print error message
        msg = error.msg if error.msg.endswith((".", "!", "?")) else f"{error.msg}."
        print(f"{'':<25}└🠆 Error: {msg}", file=sys.__stdout__, flush=True)


def validate(configuration: dict, validator: Schemable):
    """Validates the configuration against a"""
    logger = logging.getLogger("flow")
    logger.debug("Generating configuration validator...")

    assert isinstance(configuration, dict)
    configuration = configuration.copy()

    try:
        return validator(configuration)

    except Invalid as e:
        print_validation_error(validator, configuration, e)

        logger.critical("Please check your configuration or documentation for more details.")
        sys.exit(2)
