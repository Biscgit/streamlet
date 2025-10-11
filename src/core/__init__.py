"""Setup settings and logging for Streamlet before loading core modules."""

import argparse
import sys

import core.logger
from core.settings import Settings


# defined here because of import loops
def load_argv():
    """Loads commandline arguments as settings."""
    parser = argparse.ArgumentParser(description="Streamlet metric collector.")
    for k, v in Settings.__annotations__.items():
        flag = f"--{k.replace("_", "-")}"  # pylint: disable=C0103

        if hasattr(v, "__name__") and v.__name__ == "Boolean":
            parser.add_argument(flag, nargs="?", const=True, type=v, default=getattr(Settings, k))
        else:
            parser.add_argument(flag, type=v, default=getattr(Settings, k))

    for k, v in vars(parser.parse_args()).items():
        # only set if variable has been passed as argument
        if f"--{k.replace("_", "-")}" in sys.argv:
            Settings.set(k.replace("-", "_"), v, persistent=True)
