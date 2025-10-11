import logging
from unittest.mock import patch

import pytest
import redis
from celery.result import AsyncResult
from testcontainers.redis import RedisContainer

from core.flow import StreamletWorker
from core.modules import Modules
from core.settings import Settings
from core.task import StreamletTask
from tests.fixtures.modules import DebugFlow

REDIS_VERSIONS = ["8-alpine"]  # , "8-bookworm", "7"]


def run_streamlet(flow: DebugFlow):
    """Runs a provided flow."""
    worker = StreamletWorker(app=flow)

    exit_functions = []

    def mock_register(f):
        exit_functions.append(f)

    # run atexit functions after streamlet finished and not after test
    with patch("atexit.register", mock_register):
        flow.beat()
        worker.start()

    for func in exit_functions:
        func()

    task_ids = [t for _, t in StreamletTask.LAST_TASK_IDS]
    results: list[AsyncResult] = [flow.AsyncResult(_id) for _id in task_ids]

    DebugFlow.task_passed = [r for r in results if r.state != "FAILURE"]
    DebugFlow.task_failed = [r for r in results if r.state == "FAILURE"]

    return flow


@pytest.fixture(params=REDIS_VERSIONS, scope="session")
def broker(request):
    """Returns a broker for Streamlet"""
    with RedisContainer(f"redis:{request.param}") as redis:
        logging.info("Using broker Redis: %s", redis.image)
        yield redis


@pytest.fixture(autouse=True, scope="function")
def check_logs(request, caplog):
    """Automatically checks logs from stages for errors."""
    param = getattr(request, "param", [])

    level = param[0] if len(param) >= 1 else logging.ERROR
    stages = param[1] if len(param) >= 2 else ["setup", "call", "teardown"]

    yield

    # checks all logs after test has completed
    for stage in stages:
        for record in caplog.get_records(stage):
            if record.levelno >= level:
                pytest.fail(f"Message with level {record.levelno} has been sent.")


@pytest.fixture(scope="function")
def task_states(request):
    """Check the task states after running e2e."""
    states = {"passed": None, "failed": None} | getattr(request, "param", {})
    yield

    passed = states["passed"]
    if passed is not None and len(DebugFlow.task_passed) != passed:
        pytest.fail(f"{DebugFlow.task_passed} Tasks succeeded (expected: {limit})")

    limit = states["failed"] or 0
    if len(DebugFlow.task_failed) != limit:
        pytest.fail(f"{DebugFlow.task_failed} Tasks failed (expected: {limit})")


@pytest.fixture(scope="function")
def streamlet(request, broker, reset_streamlet, task_states):
    """Returns a callable Flow instance with specifiable configuration."""
    # adjust default settings for testing
    Settings.run_once = True
    Settings.hide_welcome = True
    Settings.log_file = None
    Settings.disable_readiness_probe = True
    Settings.print_traceback = True
    Settings.default_cert_path = None
    Settings.log_level = 10

    # load debug and optional modules
    Modules.initialize(file="tests.fixtures.modules")
    for module in request.param:
        Modules.initialize(file=module)

    # configure broker
    redis_host, redis_port = broker.get_container_host_ip(), broker.get_exposed_port(broker.port)
    broker_url = f"redis://{redis_host}:{redis_port}"
    Settings.celery_broker = f"{broker_url}/0"
    Settings.celery_backend = f"{broker_url}/1"

    r = redis.Redis(host=redis_host, port=redis_port)
    r.flushdb()

    yield DebugFlow
