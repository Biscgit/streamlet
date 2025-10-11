"""Logging related functions and modules."""

import logging
import sys

from core.settings import Settings


class LevelFormatter(logging.Formatter):
    """Custom formatter with short names and emojis."""

    EMOJI_LEVELS = {
        logging.DEBUG: "🔍",
        logging.INFO: "🟢",
        logging.WARNING: "🟠",
        logging.ERROR: "🔴",
        logging.CRITICAL: "🔥",
    }
    SHORT_LEVELS = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO ",
        logging.WARNING: "WARN",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRIT ",
    }

    def format(self, record):
        mod_name = getattr(logging.getLogger(record.name), "mod_name", None)

        record.emoji = self.EMOJI_LEVELS.get(record.levelno, "🐛")
        record.short_level = self.SHORT_LEVELS.get(record.levelno, "NOLVL")

        # overwrite name if custom var `mod_name` is present
        if mod_name:
            record.mod_name = f"{record.name} "
            record.name = mod_name
        else:
            record.mod_name = ""

        return super().format(record)


def get_logger(logger_name: str = "", module: str = None) -> logging.Logger:
    """Returns a basic preconfigured logger."""
    if logger_name in logging.Logger.manager.loggerDict:
        return logging.getLogger(logger_name)

    logger = logging.getLogger(logger_name)
    logger.mod_name = module
    logger.setLevel(Settings.log_level)

    return logger


def setup():
    """Sets up logging from Settings"""
    root = logging.root
    formatter = LevelFormatter(Settings.log_format)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root.addHandler(handler)

    if path := Settings.log_file:
        logging.debug("Will write logs to file %s.", path)
        handler = logging.FileHandler(path)
        handler.setFormatter(LevelFormatter(Settings.log_format))
        root.addHandler(handler)


# setup logging on first import
setup()
