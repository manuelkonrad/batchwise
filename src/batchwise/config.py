# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import os
import sys
from typing import Any

import yaml
from pydantic import model_validator
from pydantic_settings import BaseSettings

if os.path.basename(sys.argv[0]) == "pytest":
    cli_parse_args = False
else:
    cli_parse_args = True


class BatchwiseConfig(BaseSettings, cli_parse_args=cli_parse_args):  # type: ignore
    """Configuration for batch processing."""

    config_path: str | None = None
    catalog_paths: list[str] = []
    engine_paths: list[str] = []
    interval: int | None = None
    processor_default_config: dict[str, Any] = {}
    processor_configs: dict[str, dict[str, Any]] = {}

    @model_validator(mode="before")
    @classmethod
    def load_yaml(cls, values: dict) -> dict:
        """Load configuration from a YAML file if a config_path is present."""
        if values.get("config_path"):
            with open(values["config_path"], "r", encoding="utf-8") as config_file:
                file_config = yaml.safe_load(config_file)

            # init, commandline and environment args have precendence over file config
            file_config.update(values)
            return file_config
        else:
            return values


config = BatchwiseConfig()
