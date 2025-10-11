"""Entrypoint of the Celery application."""

import logging
import sys
import traceback
from importlib import metadata
from platform import freedesktop_os_release, python_version

from core import load_argv
from core.flow import StreamletFlow, StreamletWorker
from core.logger import get_logger
from core.modules import Modules
from core.settings import Settings


def main():
    """Entrypoint."""
    load_argv()
    logger = get_logger("flow")

    os_name = freedesktop_os_release().get("PRETTY_NAME", "UNKNOWN")
    logger.info(
        "Starting version %s (Python %s on %s).",
        *(metadata.version("streamlet"), python_version(), os_name),
    )

    try:
        Modules.initialize(package="modules")

        app = StreamletFlow()
        worker = StreamletWorker(app=app)

        app.beat()
        worker.start()

    # pylint: disable=W0718
    except Exception as e:
        if Settings.print_traceback:
            traceback.print_tb(e.__traceback__)

        logging.critical("[%s > %s] Failed to setup.", e.__class__.__module__, e.__class__.__name__)
        logging.critical("%s", str(e))

        sys.exit(1)


if __name__ == "__main__":
    main()
