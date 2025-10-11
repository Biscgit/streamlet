import copy

from core.abstract import AbstractModule
from core.task import StreamletTask
from tests.fixtures.e2e import *
from tests.fixtures.modules import *

SETTINGS_BACKUP: dict | None = None


@pytest.fixture(scope="function", autouse=True)
def reset_streamlet():
    """Reset modules, settings, flow and tasks for each test automatically."""
    # Create backup for default Settings
    global SETTINGS_BACKUP

    if SETTINGS_BACKUP is None:
        SETTINGS_BACKUP = {
            k: copy.deepcopy(getattr(Settings, k)) for k in Settings.__annotations__.keys()
        }

    # wait for test to finish
    yield

    # Settings
    Settings.Persistent.keys.clear()
    for k, v in SETTINGS_BACKUP.items():
        Settings.set(k, v)

    # Modules
    Modules.input_modules.clear()
    Modules.transform_modules.clear()
    Modules.output_modules.clear()
    AbstractModule.all_modules.clear()

    # Flow
    StreamletFlow.instance = None
    StreamletFlow.ready = False

    # Task
    StreamletTask.ALL_CLS.clear()
    StreamletTask.LAST_TASK_IDS = None

    # Task stats
    DebugFlow.task_passed = []
    DebugFlow.task_failed = []
