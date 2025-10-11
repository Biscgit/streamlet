"""File for Helper methods."""

import logging
import os
import threading
import typing
from http.server import BaseHTTPRequestHandler, HTTPServer

from celery.schedules import crontab

if typing.TYPE_CHECKING:
    from core.flow import StreamletFlow


def flatten(mapping: dict | list, sep: str = None, chain: list = None) -> dict:
    """Flattens nested attribute dict on defined seperator."""
    flattened = {}

    for k, v in iter(mapping.items() if isinstance(mapping, dict) else enumerate(mapping)):
        k_chain = (chain or []) + [k]
        if isinstance(v, (dict, list)):
            flattened |= flatten(v, sep, k_chain)
        else:
            flattened[sep.join([str(k) for k in k_chain]) if sep else tuple(k_chain)] = v

    return flattened


def parse_cron(cron_str: str) -> crontab:
    """Returns a celery crontab object from a cron string."""
    parts = dict(enumerate(cron_str.split(" ")))

    fields = ["minute", "hour", "day_of_month", "month_of_year", "day_of_week"]
    return crontab(**{name: parts.get(i, "*") for i, name in enumerate(fields)})


def load_env_pairs(items: dict) -> None:
    """Loads data pairs as environment variables."""
    original = os.environ.copy()
    os.environ |= items

    # pylint: disable=C3001
    check = lambda k: k not in original or (k in original and original[k] != os.environ[k])
    modified = len([k for k in os.environ if check(k)])

    if m := modified:
        logging.debug("Modified %s environment variable%s.", m, "s" if m != 1 else "")


class ReadinessEndpoint(BaseHTTPRequestHandler):
    """Base handler for requests."""

    flow = None

    # pylint: disable=C0103
    def do_GET(self):
        """Handles requests for readiness."""
        message = (404, "unknown")
        if self.path == "/readyz":
            if ReadinessEndpoint.flow.ready:
                message = (200, "ready")
            else:
                message = (503, "not ready")

        self.send_response_only(*message)
        self.end_headers()


def start_readiness_server(flow: "StreamletFlow", port: int):
    """Creates a daemon thread for status requests."""
    ReadinessEndpoint.flow = flow

    def target():
        server = HTTPServer(("0.0.0.0", port), RequestHandlerClass=ReadinessEndpoint)
        server.serve_forever()

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread
