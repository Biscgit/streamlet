import typing

from core.flow import StreamletFlow
from core.modules import AbstractInput, AbstractOutput, Importable
from core.validation import AlwaysList, JsonString

if typing.TYPE_CHECKING:
    from core.data import MetricFrame


class DebugFlow(StreamletFlow):
    """Debug Flow with predefined settings"""

    task_passed = []
    task_failed = []

    def __init__(self, configuration: dict):
        self.patched_config = configuration
        self.debug_output = []

        super().__init__()

    def get_configuration(self):
        return self.patched_config


@Importable
class DebugInput(AbstractInput):
    """Sets a specified set of data."""

    def __call__(self, params: dict) -> list[dict]:
        return params["payload"]

    @classmethod
    def task_params_schema(cls) -> dict:
        """placeholder"""
        return {"payload": AlwaysList(JsonString())}


@Importable
class DebugOutput(AbstractOutput):
    """Writes output metrics to the `debug_output` variable."""

    def __call__(self, data: "MetricFrame"):
        flow: DebugFlow = self.flow
        flow.debug_output.append(data)
