"""This file contains modules for EOS statistics."""

import os
import re
import subprocess
from fnmatch import fnmatch
from glob import glob

import simplejson as json
from voluptuous import Any, Maybe, Optional

from core.abstract import AbstractInput
from core.modules import Importable
from core.validation import AlwaysList, EnvironmentVar


@Importable
class EOSDirectoryQuota(AbstractInput):
    """An input module that collects EOS stats for a specific directory."""

    EOS_FIELDS = [
        "maxbytes",
        "maxfiles",
        "maxlogicalbytes",
        "percentageusedbytes",
        "usedbytes",
        "usedfiles",
        "usedlogicalbytes",
    ]
    EOS_ATTRIBUTES = [
        "gid",
        "quota",
        "space",
        "statusbytes",
        "statusfiles",
    ]
    OPT_ATTRIBUTES = [
        "pattern",
    ]

    def __init__(self, *args):
        super().__init__(*args)
        self.eos_url = self.connection_config["eos_url"]

    def on_connect(self) -> None:
        if self.eos_url is not None:
            os.putenv("EOS_MGM_URL", self.eos_url)

        try:
            proc = subprocess.run(["eos", "--version"], capture_output=True, check=True)

        except Exception as e:
            self.logger.error("The EOS command is not available on this system.")
            raise RuntimeError("Failed to run EOS command.") from e

        # --version is being written to stderr instead of stdout
        out = proc.stderr.decode().strip().splitlines()[0]
        version, year = re.match(r"^EOS (.*) \((\d+)\)$", out).groups()

        try:
            proc = subprocess.run(["eos", "whoami"], capture_output=True, check=True)

        except Exception as e:
            self.logger.error("EOS cannot connect to the filesystem: %s.", proc.stderr.decode())
            raise RuntimeError("Failed to run EOS command.") from e

        whoami = proc.stdout.decode().strip()
        self.logger.info(
            "Connected to EOS (v%s, %s) on %s with uid %s.",
            *(version, year, self.eos_url, whoami.split("uid=")[1].split(" ")[0]),
        )

    @classmethod
    def connection_schema(cls) -> dict:
        """
        :eos_url: EOS_MGM_URL to be used for EOS. Set to None to use default
        """
        return {Optional("eos_url", default="root://eospublic.cern.ch"): Maybe(EnvironmentVar())}

    @classmethod
    def task_params_schema(cls) -> dict:
        """
        :pattern: One or more paths to folder on EOS. Use unix patterns to include all that match
        :eos_metrics: Metrics taken from eos quota command
        :eos_attributes: Attributes taken from eos quota command and task configuration
        :skip_paths: Skip multiple folders by using folder names and/or unix-like patterns
        """
        # pylint: disable=E1120
        attrs = cls.EOS_ATTRIBUTES + cls.OPT_ATTRIBUTES
        return {
            "pattern": AlwaysList(EnvironmentVar()),
            Optional("eos_metrics", default=cls.EOS_FIELDS): AlwaysList(Any(*cls.EOS_FIELDS)),
            Optional("eos_attributes", default=attrs): AlwaysList(Any(*attrs)),
            Optional("skip_paths", default=[]): AlwaysList(),
        }

    def process_quota(self, raw, pattern, params):
        """Process EOS quota result"""
        eos_attrs = [p for p in params["eos_attributes"] if p in self.EOS_ATTRIBUTES]
        key_attrs = [p for p in params["eos_attributes"] if p in self.OPT_ATTRIBUTES]

        for data in raw["result"]:
            for m_key in params["eos_metrics"]:
                doc = {
                    "metric": data[m_key],
                    "eos_metric_key": m_key,
                    **{k: v for k, v in data.items() if k in eos_attrs},
                }
                if "pattern" in key_attrs:
                    doc["pattern"] = pattern

                yield doc

    def __call__(self, params: dict):
        skip = params["skip_paths"]
        result = []

        for pattern in params["pattern"]:
            folders = [p for p in glob(pattern) if os.path.isdir(p)]

            for folder in folders:
                if any(fnmatch(folder, f) for f in skip):
                    continue

                args = ["eos", "-j", "quota", folder]
                proc = subprocess.run(args, capture_output=True, check=False)

                raw = json.loads(proc.stdout)

                # catch no quota for empty folders
                if proc.returncode == 0:
                    eos_quota = list(self.process_quota(raw, pattern, params))
                    result.extend(eos_quota)

                elif proc.returncode == 22:
                    self.logger.warning("Folder %s has no quota available.", folder)

                else:
                    self.logger.error(
                        "Failed to retrieve quota for %s: %s.", folder, raw["errormsg"]
                    )

        return result
