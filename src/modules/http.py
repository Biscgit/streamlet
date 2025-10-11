"""This file contains modules for HTTP requests."""

import abc
import typing
from abc import abstractmethod

import requests
import simplejson as json
from requests.auth import HTTPBasicAuth
from voluptuous import All, Any, Boolean, Lower, Maybe, Optional, PathExists, Switch

from core import Settings
from core.abstract import AbstractInput, AbstractModule, AbstractOutput
from core.modules import Importable
from core.validation import AlwaysList, EnvironmentVar, TimeToSeconds

METHODS = ["get", "post", "put", "delete", "head", "options", "patch"]

if typing.TYPE_CHECKING:
    from core.metric import MetricFrame


# --- Base classes


class BaseHTTP(AbstractModule, abc.ABC):
    """Base class for HTTP modules."""

    def __init__(self, *args):
        super().__init__(*args)
        self._session = requests.Session()

    def on_connect(self) -> None:
        if auth := self.connection_config["auth"]:
            if bearer := auth.get("bearer_token"):
                self._session.headers["Authorization"] = f"Bearer {bearer['token']}"

            else:
                self._session.auth = HTTPBasicAuth(auth["username"], auth["password"])

        if headers := self.connection_config["headers"]:
            self._session.headers.update(headers)

    def on_shutdown(self) -> None:
        self._session.close()
        self.logger.info("Closed HTTP session.")

    @classmethod
    def connection_schema(cls) -> dict:
        auth = Switch(
            {"username": EnvironmentVar(), "password": EnvironmentVar()},
            {"bearer_token": EnvironmentVar()},
        )
        # pylint: disable=E1120
        return {
            "endpoint": EnvironmentVar(),
            Optional("auth", default=None): Maybe(auth),
            Optional("headers", default={}): Optional({str: str}),
            "method": Any(*METHODS),
            Optional("allow_redirects", default=False): Boolean(),
            Optional("allowed_responses", default=[200]): AlwaysList(int),
            Optional("cert_path", default=Settings.default_cert_path): Maybe(PathExists()),
        }

    def _http_call(self, path: str, timeout: int, payload=None) -> requests.Response:
        response = self._session.request(
            method=self.connection_config["method"].upper(),
            url=f"{self.connection_config["endpoint"]}{path}",
            verify=self.connection_config["cert_path"],
            allow_redirects=self.connection_config["allow_redirects"],
            timeout=timeout,
            data=payload,
        )

        if response.status_code not in self.connection_config["allowed_responses"]:
            self.logger.error(
                "HTTP request failed with status code %s: %s",
                response.status_code,
                response.text,
            )

            response.status_code = max(400, response.status_code)
            response.raise_for_status()

        return response


class BaseHTTPInput(AbstractInput, BaseHTTP):
    """Base class for HTTP input modules."""

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :path: Request path of the Endpoint
        :timeout: Request timeout
        """
        return {
            Optional("path", default=""): str,
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
        }

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: Endpoint to connect to
        :auth: Basic auth with username and password or bearer token
        :headers: Additional request headers
        :method: HTTP Method of the Request
        :allow_redirects: Allow requests being redirected
        :allowed_responses: Response codes that mark the task as success
        :cert_path: Path to certificate
        """
        s = super().connection_schema()
        del s["method"]
        return s | {Optional("method", default="get"): All(Lower, Any(*METHODS))}

    def __call__(self, params: dict) -> requests.Response:
        return self._http_call(params["path"], params["timeout"])


class BaseHTTPOutput(AbstractOutput, BaseHTTP):
    """Base class for HTTP output modules."""

    @classmethod
    def params_schema(cls) -> dict:
        """
        :path: Request path of the Endpoint
        :timeout: Request timeout
        """
        return {
            Optional("path", default=""): str,
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
        }

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: Endpoint to connect to
        :auth: Basic auth with username and password or bearer token
        :headers: Additional request headers
        :method: HTTP Method of the Request
        :allow_redirects: Allow requests being redirected
        :allowed_responses: Response codes that mark the task as success
        :cert_path: Path to certificate
        """
        s = super().connection_schema()
        del s["method"]
        return s | {Optional("method", default="post"): All(Lower, Any(*METHODS))}

    def __call__(self, data: "MetricFrame") -> requests.Response:
        payload = self.format_payload(data)
        return self._http_call(self.settings["path"], self.settings["timeout"], payload=payload)

    @abstractmethod
    def format_payload(self, data: "MetricFrame"):
        """Format the payload before sending."""


# --- Implementations


@Importable
class RawHTTPInput(BaseHTTPInput, AbstractInput):
    """Input module that retrieves a raw value.
    Can be used for raw texts or direct metrics by setting it to the metric field."""

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :path: Request path of the Endpoint
        :timeout: Request timeout
        :response_field: Attribute field to which the raw value gets stored at
        """
        s = super().task_params_schema()
        return s | {Optional("response_field", default="message"): str}

    def __call__(self, params: dict):
        request = super().__call__(params)
        return {params["response_field"]: request.text}


@Importable()
class JsonHTTPInput(BaseHTTPInput, AbstractInput):
    """An input module that retrieves JSON data from an HTTP endpoint."""

    def __call__(self, params: dict):
        request = super().__call__(params)
        return request.json()


@Importable()
class BasicHTTPOutput(BaseHTTPOutput, AbstractOutput):
    """An output module that sends data to an HTTP endpoint."""

    @classmethod
    def params_schema(cls) -> dict:
        """
        :path: Request path of the Endpoint
        :timeout: Request timeout
        :payload_format: Set the format of the payload
        """
        s = super().params_schema()
        return s | {
            Optional("payload_format", default="json"): All(Lower, Any("json")),
        }

    def format_payload(self, data: "MetricFrame"):
        payload_format = self.settings["payload_format"]
        if payload_format == "json":
            return json.dumps(data, tuple_as_array=True, use_decimal=True)

        raise ValueError(f"Unsupported payload format: {payload_format}")
