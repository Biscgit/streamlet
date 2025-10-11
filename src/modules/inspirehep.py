"""This file contains reads from INSPIRE-HEP."""

import requests
from voluptuous import Optional

from core.modules import AbstractInput, Importable
from core.validation import EnvironmentVar


@Importable
class InspireCitationsInput(AbstractInput):
    """Reads and paginates through Inspire."""

    INSPIRE_RATE_LIMIT = "15/5s"

    @classmethod
    def module_name(cls, lower: bool = True):
        return "inspirecitations" if lower else "InspireCitations"

    def __init__(self, *args):
        super().__init__(*args)
        self._session = requests.Session()
        self.endpoint = self.connection_config["endpoint"]

    def on_shutdown(self) -> None:
        self._session.close()
        self.logger.info("Closed Inspire HTTP session.")

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :endpoint: Full API url for InspireHep search
        """
        return {Optional("endpoint", default="https://inspirehep.net/api/literature"): str}

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :q: URL search query from InspireHep
        """
        return {"q": EnvironmentVar()}

    def __call__(self, params: dict):
        url = f"{self.endpoint}?size=1&fields=citation_count&q={params["q"]}"
        request = self._session.get(url).json()

        return {"metric": request["hits"]["total"]}
