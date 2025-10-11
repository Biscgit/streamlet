"""This file contains modules related to CERN Monit Infrastructure."""

import threading
import time
import typing

import simplejson as json
from voluptuous import Boolean, Maybe, Optional, PathExists

from core import Settings
from core.modules import Importable
from core.validation import AlwaysList, EnvironmentVar, TimeToSeconds

from .http import BaseHTTPOutput
from .opentelemetry import OpenTelemetryABC

if typing.TYPE_CHECKING:
    from core.metric import MetricFrame


@Importable()
class OpenTelemetryMONIT(OpenTelemetryABC):
    """An output module that sends data to the MONIT's OpenTelemetry endpoint."""

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: MONIT's HTTP Endpoint
        :producer: Sets the producer. If None, then the username will be used automatically
        :auth: Pair of username and password for authentication
        :headers: Additional request headers
        :cert_path: Path to certificate
        """
        # pylint: disable=E1120
        return {
            Optional("endpoint", default="https://monit-otlp.cern.ch:4319/v1/metrics"): str,
            Optional("producer", default=None): Maybe(EnvironmentVar()),
            "auth": {"username": EnvironmentVar(), "password": EnvironmentVar()},
            Optional("headers", default={}): Optional({str: str}),
            Optional("cert_path", default=Settings.default_cert_path): Maybe(PathExists()),
        }


@Importable()
class MonitMetricOutput(BaseHTTPOutput):
    """An output module that sends data to the MONIT's HTTP endpoint."""

    def __init__(self, *args):
        super().__init__(*args)
        conn_conf = self.connection_config
        self.producer = conn_conf["producer"] or conn_conf["auth"]["username"]
        self.settings["path"] = f"/{self.producer}"

        self.rl_lock = threading.RLock()
        self.last_reset = time.time()

        try:
            size, period = conn_conf["rate_limit"].split("/", maxsplit=1)
            self.batch_size = int(size)
            self.sleep_time = TimeToSeconds()(period)

        except Exception as e:
            self.logger.error("Rate format is not in int/time but `%s`.", conn_conf["rate_limit"])
            raise e

        self.frame_filled = 0

    def __call__(self, data: "MetricFrame"):
        # MONIT's endpoint has a limit of 10000 entries per request
        while len(data) > 0:
            with self.rl_lock:
                available = self.batch_size - self.frame_filled

                # ensure rate limit while keeping speed
                if available < len(data):
                    if time.time() < (self.last_reset + self.sleep_time):
                        sleep_time = self.sleep_time - (time.time() - self.last_reset)
                        time.sleep(sleep_time)

                    self.frame_filled = 0
                    self.last_reset = time.time()
                    available = self.batch_size

                data_slice = data[:available]
                data = data[available:]

                self.frame_filled += len(data_slice)

            payload = self.format_payload(data_slice)

            # ToDo: # handle monit infra errors (http 500)?
            self.logger.debug("Sending payload [s=%d] to endpoint.", len(data_slice))
            self._http_call(self.settings["path"], self.settings["timeout"], payload=payload)

    def format_payload(self, data: "MetricFrame"):
        timestamp = data.creation_timestamp.timestamp()
        payload = [
            {
                "producer": self.producer,
                "type": data.name,
                "type_prefix": self.settings["monit_type_prefix"],
                "environment": self.settings["environment"],
                "timestamp": timestamp,
                "data": dict(metric),
                **self.settings["static_attributes"],
            }
            for metric in data
        ]

        self.logger.debug("Dumped metrics to Monit OpenSearch format.")
        return json.dumps(payload, allow_nan=True, sort_keys=True)

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: MONIT's HTTP Endpoint
        :producer: Sets the producer. If None, then the username will be used automatically
        :auth: Pair of username and password for authentication
        :method: HTTP Method of the Request
        :headers: Additional request headers
        :allow_redirects: Allow requests being redirected
        :allowed_responses: Response codes that mark the task as success
        :cert_path: Path to certificate
        :rate_limit: Send up to x many documents in a time frame over multiple requests.
        """
        # pylint: disable=E1120
        return {
            Optional("endpoint", default="https://monit-metrics.cern.ch:10014"): str,
            Optional("producer", default=None): Maybe(EnvironmentVar()),
            "auth": {"username": EnvironmentVar(), "password": EnvironmentVar()},
            Optional("method", default="post"): "post",
            Optional("headers", default={"Content-Type": "application/json"}): {str: str},
            Optional("allow_redirects", default=False): Boolean(),
            Optional("allowed_responses", default=[200]): AlwaysList(int),
            Optional("cert_path", default=Settings.default_cert_path): Maybe(PathExists()),
            Optional("rate_limit", default="2000/500ms"): EnvironmentVar(),
        }

    @classmethod
    def params_schema(cls) -> dict:
        """
        :environment: Set metadata environment for the request
        :static_attributes: A set of additional attributes to be added
        :monit_type_prefix: Do not set except required. Check MONIT docs first
        :path: Do not set. It will automatically map to producer
        :timeout: Request timeout
        :original_field_name: Set a field to store the original metric's name
        """
        return {
            Optional("environment", default="default"): EnvironmentVar(),
            Optional("static_attributes", default={}): Optional({str: object}),
            Optional("monit_type_prefix", default="raw"): str,
            Optional("path", default=None): Maybe(str),
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
            Optional("original_field_name", default=None): Maybe(str),
        }
