"""This file contains modules for Open Telemetry endpoints."""

import abc
import typing

import requests
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from voluptuous import Maybe, Optional, PathExists

from core.abstract import AbstractOutput
from core.modules import Importable
from core.validation import EnvironmentVar, TimeToSeconds

if typing.TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal.instrument import Gauge

    from core.metric import MetricFrame


class OpenTelemetryABC(AbstractOutput, abc.ABC):
    """ABC base implementations for OpenTelemetry output modules."""

    def __init__(self, *args):
        super().__init__(*args)
        params = {}

        if cert_path := self.connection_config["cert_path"]:
            params["certificate_file"] = cert_path

        self.producer = self.connection_config["producer"]
        self._session = requests.Session()
        self._session.headers.update(self.connection_config["headers"] | {"Connection": "close"})

        if auth := self.connection_config["auth"]:
            self._session.auth = (auth["username"], auth["password"])
            self.producer = self.producer or auth["username"]

        self.exporter = OTLPMetricExporter(
            endpoint=self.connection_config["endpoint"], session=self._session, **params
        )
        self.reader = PeriodicExportingMetricReader(self.exporter)

        self.provider = MeterProvider(
            metric_readers=[self.reader],
            resource=Resource.create({"service.name": self.producer}),
        )
        self.meter = self.provider.get_meter(f"streamlet.{self.producer}")
        self.instruments: dict[str, "Gauge"] = {}

    def on_shutdown(self) -> None:
        self.exporter.force_flush()
        self.exporter.shutdown()
        self._session.close()

        self.logger.info("Closed OpenTelemetry exporter.")

    def __call__(self, data: "MetricFrame"):
        instrument_name = f"{self.producer}.{data.name}"

        if not (instrument := self.instruments.get(instrument_name)):
            instrument = self.meter.create_gauge(instrument_name)
            self.instruments[instrument_name] = instrument

        for metric in data:
            attributes = metric.flatten() | self.settings["static_attributes"]

            if metric_field_name := self.settings["metric_field_name"]:
                attributes[metric_field_name] = metric.metric_field_name

            instrument.set(metric.metric, attributes=attributes)

        if timeout := self.settings["flush_timeout"]:
            if not self.provider.force_flush(timeout * 1000):
                self.logger.warning("Failed to flush OpenTelemetry metrics.")

    @classmethod
    def params_schema(cls) -> dict:
        """
        :metric_field_name: Set a field to store the original metric's name. Included by default
        :flush_timeout: Force flush all metrics after specified time
        :static_attributes: A set of additional attributes to be added
        """
        return {
            Optional("metric_field_name", default="sample_name"): Maybe(str),
            Optional("flush_timeout", default=None): Maybe(TimeToSeconds()),
            Optional("static_attributes", default={}): Optional({str: object}),
        }


@Importable()
class OpenTelemetryHTTP(OpenTelemetryABC):
    """An output module that sends data to an HTTP endpoint in the OpenTelemetry format."""

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: Endpoint to send Metrics to
        :producer: Name of the producer
        :auth: Pair of username and password for authentication
        :headers: Additional request headers
        :cert_path: Path to certificate
        """
        auth = {"username": EnvironmentVar(), "password": EnvironmentVar()}
        # pylint: disable=E1120
        return {
            "endpoint": EnvironmentVar(),
            "producer": EnvironmentVar(),
            Optional("auth", default=None): Maybe(auth),
            Optional("headers", default={}): Optional({str: str}),
            Optional("cert_path", default=None): EnvironmentVar(PathExists()),
        }
