"""This file contains modules for OpenSearch requests."""

import abc
import base64
import logging
import threading
import typing
from urllib.parse import urlparse

from opensearchpy import NotFoundError, OpenSearch
from opensearchpy.helpers import bulk
from voluptuous import All, Any, Boolean, Coerce, Lower, Maybe, Optional, PathExists, Union

from core import Settings
from core.abstract import AbstractInput, AbstractModule, AbstractOutput, AbstractTransform
from core.modules import Importable
from core.validation import AlwaysList, EnvironmentVar, JsonString, TimeToSeconds

if typing.TYPE_CHECKING:
    from core.metric import MetricFrame

opensearch_logger = logging.getLogger("opensearch")
opensearch_logger.setLevel(level=logging.ERROR)
opensearch_logger.propagate = False


class BaseOpenSearch(AbstractModule, metaclass=abc.ABCMeta):
    """Base class for OpenSearch based modules."""

    def __init__(self, *args):
        super().__init__(*args)

        self._connection: OpenSearch | None = None
        self._url_string = ""

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :port: Database port
        :auth: Basic auth with username and password
        :url_prefix: Set a url prefix to requests
        :use_ssl: Use SSL for the connection
        :cert_path: Path to certificate
        :verify_certs: Verify certificate
        :http_compress: Compress HTTP traffic
        :startup_ping: Ping instance on startup
        :pool_maxsize: Maximum number of concurrent requests
        """
        auth = {"username": Maybe(EnvironmentVar()), "password": Maybe(EnvironmentVar())}
        default_cert = Settings.default_cert_path
        # pylint: disable=E1120
        return {
            "host": EnvironmentVar(),
            Optional("port", default=None): Maybe(EnvironmentVar(int)),
            Optional("auth", default=None): Maybe(auth),
            Optional("url_prefix", default=""): EnvironmentVar(),
            Optional("use_ssl", default=True): Boolean(),
            Optional("cert_path", default=default_cert): Maybe(EnvironmentVar(PathExists())),
            Optional("verify_certs", default=True): Boolean(),
            Optional("http_compress", default=True): Boolean(),
            Optional("startup_ping", default=False): Boolean(),
            Optional("pool_maxsize", default=16): int,
        }

    def on_connect(self) -> None:
        conf = self.connection_config

        parsed = urlparse(host if "://" in (host := conf["host"]) else f"https://{host}")
        host = parsed.netloc.split(":")[0]
        port = conf["port"] or parsed.port or 443
        prefix = (conf["url_prefix"] or parsed.path or "").removeprefix("/")

        connection = {
            "hosts": [{"host": host, "port": port}],
            "url_prefix": prefix,
            "use_ssl": conf["use_ssl"],
            "ca_certs": conf["cert_path"],
            "verify_certs": conf["verify_certs"],
            "http_compress": conf["http_compress"],
            "pool_maxsize": conf["pool_maxsize"],
        }
        if auth := self.connection_config["auth"]:
            connection["http_auth"] = (auth["username"], auth["password"])

        self._url_string = f"{host}:{port}{f'/{prefix}' if prefix else ''}"
        self.logger.info(
            "Connecting with user %s to OpenSearch %s.",
            *(auth.get("username"), self._url_string),
        )
        self._connection = OpenSearch(**connection)

        if conf["startup_ping"]:
            if not self._connection.ping():
                self.logger.error("Failed to ping OpenSearch Instance")

    def on_shutdown(self) -> None:
        if self._connection is not None:
            logging.info("Closing connection to %s.", self._url_string)
            self._connection.close()

    def _check_shard_failure(self, raw_data):
        if (shard := raw_data["_shards"])["failed"] > 0:
            self.logger.error("%s of %s shards failed.", shard["failed"], shard["total"])

            for err in shard["failures"]:
                reason = err["reason"]
                self.logger.error(
                    "> Shard %s error: [%s] %s",
                    err["shard"],
                    reason["type"],
                    reason["reason"],
                )

            raise ValueError("OpenSearch query failed.")

    def _full_bucket_unpack(self, data: dict):
        """Unpacks the data from the OpenSearch query response."""
        is_finished = True

        for bucket_name, value in data.copy().items():
            if isinstance(value, dict):
                buckets = value.get("buckets")

                if buckets is not None:
                    is_finished = False
                    base = {k: v for k, v in data.items() if k != bucket_name}

                    if isinstance(buckets, list):

                        if len(buckets):
                            for bucket in buckets:
                                bucket[bucket_name] = bucket.pop("key")

                                # unnest fields with 'value'
                                for nk, nv in bucket.items():
                                    if isinstance(nv, dict) and ("value" in nv.keys()):
                                        bucket[nk] = nv["value"]

                                yield from self._full_bucket_unpack(base | bucket)

                        # yield in case `buckets` is an empty list
                        else:
                            yield base

                    # special case when using filters
                    elif isinstance(buckets, dict):
                        for key, bucket in buckets.items():
                            # unnest fields with 'value'
                            for nk, nv in bucket.items():
                                if isinstance(nv, dict) and ("value" in nv.keys()):
                                    bucket[nk] = nv["value"]

                            bucket[bucket_name] = key
                            yield from self._full_bucket_unpack(base | bucket)

        if is_finished:
            yield data


@Importable()
class OpenSearchInput(BaseOpenSearch, AbstractInput):
    """An input module that retrieves data from an OpenSearch database.
    Check OpenSearch docs for more on querying the database.

    This module supports two main modes or querying:

    - Dev Tools based queries:
      Set the fields `queries` to the `query` field and `aggs` to `aggs` from the query
      As it is common to use multiple filters at once, instead of setting filters->bool,
      it is possible to define multiple query filters by passing an array of queries.

    - Dashboard based queries:
      Similar to the dashboard queries, set the time range and use Lucene to filter for fields.
      Set the field `lucene` directly, or the nested fields according to OpenSearch docs.
    """

    @classmethod
    def task_params_schema(cls) -> Union:
        """
        :index: Index for searching
        :lucene: Query using Apache Lucene or `query` with optional fields.
        :query: JSON argument for the query parameter (deprecated)
        :queries: One or a list of JSON arguments for queries that will be connected by boolean AND
        :aggs: JSON argument for the aggs parameter
        :size: Number of raw documents returned. Not used with aggregations.
        :result: Query result part that gets mapped as Metric
        :timeout: Request timeout
        :timerange: Define the time range easily instead of in the query, use opensearch syntax
        """
        base = {
            "index": EnvironmentVar(),
            Optional("aggs", default=None): Maybe(JsonString(load=True)),
            Optional("size", default=0): Coerce(int),
            Optional("result", default="aggregation"): All(Lower, Any("aggregation", "documents")),
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
            Optional("timerange", default=None): Maybe(
                {
                    "from": str,
                    Optional("to", default="now"): str,
                    Optional("from_handler", default="gte"): Any("gt", "gte"),
                    Optional("to_handler", default="lt"): Any("lt", "lte"),
                    Optional("timestamp_field", default="metadata.timestamp"): EnvironmentVar(),
                }
            ),
        }
        lucene = {
            "query": EnvironmentVar(),
            Optional("fields", default=None): Maybe(AlwaysList(str)),
        }
        return Union(
            {"lucene": Union(lucene, EnvironmentVar())} | base,
            {
                Optional("query", default=None): Maybe(JsonString(load=True)),
                Optional("queries", default=None): Maybe(AlwaysList(JsonString(load=True))),
            }
            | base,
        )

    def __call__(self, params: dict) -> list[dict]:
        body = {"size": params["size"]}
        filters = []

        if lucene := params.get("lucene"):
            if isinstance(lucene, str):
                lucene = {"query": lucene}

            config = {k: v for k, v in lucene.items() if v is not None}
            filters.append({"query_string": config})

        elif query := params.get("query"):
            self.logger.warning("Use `queries` parameter instead of deprecated `query`")
            filters.append(query)

        elif queries := params.get("queries"):
            filters.extend(queries)

        if aggs := params["aggs"]:
            body["aggs"] = aggs

        if timerange := params["timerange"]:
            range_filter = {
                timerange["timestamp_field"]: {
                    timerange["from_handler"]: timerange["from"],
                    timerange["to_handler"]: timerange["to"],
                }
            }
            filters.append({"range": range_filter})

        if filters:
            body["query"] = {"bool": {"filter": filters}}

        # pylint: disable=E1123
        raw_data = self._connection.search(
            body=body,
            index=params["index"],
            timeout=params["timeout"],
        )

        self._check_shard_failure(raw_data)

        if params["result"] == "aggregation":
            if raw_data["hits"]["total"]["value"] == 0:
                self.logger.debug("No documents found for the query.")
                return []

            itr = self._full_bucket_unpack(raw_data["aggregations"])
            return list(itr)

        if params["result"] == "documents":
            hits = raw_data["hits"]

            if hits["total"]["relation"] != "eq":
                self.logger.warning(
                    "Query exceeded limit of %s documents. Some might be missing.",
                    hits["total"]["value"],
                )

            return hits["hits"]

        self.logger.error("Unknown result handling mode: %s.", params["result"])
        raise NotImplementedError("Unknown result handling mode.")


@Importable()
class OpenSearchIndexMapping(BaseOpenSearch, AbstractTransform):
    """A transformer module that enriches the data from an OpenSearch database mapping."""

    def __init__(self, *args):
        super().__init__(*args)
        self.encoders = {
            "b64": lambda t: base64.b64encode(t.encode("utf-8")).decode(),
            "b64url": lambda t: base64.urlsafe_b64encode(t.encode("utf-8")).decode(),
        }

    @classmethod
    def params_schema(cls) -> dict:
        """
        :search_index: Index for searching
        :search_index_key: Attribute from the Metric for searching as document key
        :include_attributes: Attributes to be included from the fetched document. All by default
        :default_values: Default values for attributes if doc is not found
        :timeout: Request timeout
        :key_encode: Encode the search_index_key
        :fail_on_not_found: Raise an exception if query fails
        """
        # pylint: disable=E1120
        return {
            "search_index": str,
            "search_index_key": str,
            Optional("include_attributes", default=None): Maybe(AlwaysList(str)),
            Optional("default_values", default={}): Optional({str: object}),
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
            Optional("key_encode", default=None): Maybe(All(Lower, Any("", "b64", "b64url"))),
            Optional("fail_on_not_found", default=False): Boolean(),
        }

    def __call__(self, data: "MetricFrame"):

        defaults = self.settings["default_values"]
        include_fields = self.settings["include_attributes"]

        for metric in data:
            key = metric[self.settings["search_index_key"]]

            if encoding := self.settings["key_encode"]:
                key = self.encoders[encoding](key)

            try:
                response = self._connection.get(index=self.settings["search_index"], id=key)
                document: dict = response["_source"]

            except (NotFoundError, KeyError) as e:
                if self.settings["fail_on_not_found"]:
                    raise e
                document = {}

            if include_fields is not None:
                document = {key: document.get(key) or defaults.get(key) for key in include_fields}

            metric |= document


@Importable()
class OpenSearchOutput(BaseOpenSearch, AbstractOutput):
    """An output module that sends data to an OpenSearch database."""

    def __init__(self, *args):
        super().__init__(*args)
        self._write_lock = threading.Lock()

    @classmethod
    def params_schema(cls) -> dict:
        """
        :index: Index to insert documents in
        :timestamp_field: Field to set the timestamp as. Not included by default
        :timeout: Request timeout
        """
        return {
            "index": EnvironmentVar(),
            Optional("timestamp_field", default=None): Maybe(str),
            Optional("timeout", default=60): EnvironmentVar(TimeToSeconds()),
        }

    def __call__(self, data: "MetricFrame"):
        if timestamp_field := self.settings["timestamp_field"]:
            ts = data.creation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            for metric in data:
                metric[timestamp_field] = ts

        index = self.settings["index"]
        payload = [{"_index": index, "_source": dict(m)} for m in data]

        with self._write_lock:
            count, _ = bulk(
                self._connection,
                payload,
                request_timeout=self.settings["timeout"],
                chunk_size=256,
            )

        if count < len(data):
            raise ValueError(f"Failed to insert {len(data) - count} documents into index {index}.")
